package storagemarket

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/application-research/estuary/boost/db"
	"github.com/application-research/estuary/boost/db/migrations"
	"github.com/application-research/estuary/boost/storagemanager"
	"github.com/application-research/estuary/boost/storagemarket/logs"
	"github.com/application-research/estuary/boost/storagemarket/types"
	smtypes "github.com/application-research/estuary/boost/storagemarket/types"
	"github.com/application-research/estuary/boost/storagemarket/types/dealcheckpoints"
	"github.com/application-research/estuary/boost/transport"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var (
	ErrDealNotFound        = fmt.Errorf("deal not found")
	ErrDealHandlerNotFound = errors.New("deal handler not found")
)

type Config struct {
	// The maximum amount of time a transfer can take before it fails
	MaxTransferDuration time.Duration
}

type ProviderDealRejectionInfo struct {
	Accepted bool
	Reason   string // The rejection reason, if the deal is rejected
}

var log = logging.Logger("boost-provider")

type Provider struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeSync sync.Once
	runWG     sync.WaitGroup

	newDealPS *newDealPS

	// channels used to pass messages to run loop
	acceptDealChan   chan acceptDealReq
	finishedDealChan chan finishedDealReq
	storageSpaceChan chan storageSpaceDealReq

	// Database API
	db        *sql.DB
	dealsDB   *db.DealsDB
	logsSqlDB *sql.DB

	Transport      transport.Transport
	storageManager *storagemanager.StorageManager
	transfers      *dealTransfers

	dhsMu sync.RWMutex
	dhs   map[uuid.UUID]*dealHandler // Map of deal handlers indexed by deal uuid.

	dealLogger *logs.DealLogger
}

func NewProvider(sqldb *sql.DB, dealsDB *db.DealsDB, storageMgr *storagemanager.StorageManager,
	logsSqlDB *sql.DB, dl *logs.DealLogger, tspt transport.Transport) (*Provider, error) {

	newDealPS, err := newDealPubsub()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &Provider{
		ctx:       ctx,
		cancel:    cancel,
		newDealPS: newDealPS,
		db:        sqldb,
		dealsDB:   dealsDB,
		logsSqlDB: logsSqlDB,

		acceptDealChan:   make(chan acceptDealReq),
		finishedDealChan: make(chan finishedDealReq),
		storageSpaceChan: make(chan storageSpaceDealReq),

		Transport:      tspt,
		storageManager: storageMgr,

		transfers: newDealTransfers(),

		dhs:        make(map[uuid.UUID]*dealHandler),
		dealLogger: dl,
	}, nil
}

func (p *Provider) NBytesReceived(dealUuid uuid.UUID) uint64 {
	return p.transfers.getBytes(dealUuid)
}

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := p.dealsDB.ByID(ctx, dealUuid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

// ExecuteDeal is called when the Storage Provider receives a deal proposal
// from the network
func (p *Provider) ExecuteDeal(dp *types.DealParams, clientPeer peer.ID) (*ProviderDealRejectionInfo, error) {
	p.dealLogger.Infow(dp.DealUUID, "executing deal proposal received from network", "peer", clientPeer)

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUUID,
		ClientDealProposal: dp.ClientDealProposal,
		ClientPeerID:       clientPeer,
		DealDataRoot:       dp.DealDataRoot,
		Transfer:           dp.Transfer,
		IsOffline:          dp.IsOffline,
		Retry:              smtypes.DealRetryAuto,
	}

	return p.executeDeal(ds)
}

// executeDeal sends the deal to the main provider run loop for execution
func (p *Provider) executeDeal(ds smtypes.ProviderDealState) (*ProviderDealRejectionInfo, error) {
	ri, err := func() (*ProviderDealRejectionInfo, error) {
		// send the deal to the main provider loop for execution
		resp, err := p.checkForDealAcceptance(&ds, false)
		if err != nil {
			p.dealLogger.LogError(ds.DealUuid, "failed to send deal for acceptance", err)
			return nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
		}

		// if there was an error, we don't return a rejection reason, just the error.
		if resp.err != nil {
			return nil, fmt.Errorf("failed to accept deal: %w", resp.err)
		}

		// log rejection reason as provider has rejected the deal.
		if !resp.ri.Accepted {
			p.dealLogger.Infow(ds.DealUuid, "deal rejected by provider", "reason", resp.ri.Reason)
		}

		return resp.ri, nil
	}()

	if err != nil || ri == nil || !ri.Accepted {
		// if there was an error processing the deal, or the deal was rejected, return
		return ri, err
	}

	if ds.IsOffline {
		p.dealLogger.Infow(ds.DealUuid, "offline deal accepted, waiting for data import")
	} else {
		p.dealLogger.Infow(ds.DealUuid, "deal accepted and scheduled for execution")
	}
	return ri, nil
}

func (p *Provider) checkForDealAcceptance(ds *types.ProviderDealState, isImport bool) (acceptDealResp, error) {
	// send message to run loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealChan <- acceptDealReq{rsp: respChan, deal: ds, isImport: isImport}:
	case <-p.ctx.Done():
		return acceptDealResp{}, p.ctx.Err()
	}

	var resp acceptDealResp
	select {
	case resp = <-respChan:
	case <-p.ctx.Done():
		return acceptDealResp{}, p.ctx.Err()
	}

	return resp, nil
}

func (p *Provider) Start() error {
	log.Infow("storage provider: starting")

	// initialize the database
	log.Infow("db: creating tables")
	err := db.CreateAllBoostTables(p.ctx, p.db, p.logsSqlDB)
	if err != nil {
		return fmt.Errorf("failed to init db: %w", err)
	}

	log.Infow("db: performing migrations")
	err = migrations.Migrate(p.db)
	if err != nil {
		return fmt.Errorf("failed to migrate db: %w", err)
	}

	log.Infow("db: initialized")

	// cleanup all completed deals in case Boost resumed before they were cleanedup
	finished, err := p.dealsDB.ListCompleted(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to list completed deals: %w", err)
	}
	if len(finished) > 0 {
		log.Infof("cleaning up %d completed deals", len(finished))
	}
	for i := range finished {
		p.cleanupDealOnRestart(finished[i])
	}
	if len(finished) > 0 {
		log.Infof("finished cleaning up %d completed deals", len(finished))
	}

	// restart all active deals
	activeDeals, err := p.dealsDB.ListActive(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to list active deals: %w", err)
	}

	// cleanup all deals that have finished successfully
	for _, deal := range activeDeals {
		// Make sure that deals that have reached the index and announce stage
		// have their resources untagged
		// TODO Update this once we start listening for expired/slashed deals etc
		if deal.Checkpoint >= dealcheckpoints.IndexedAndAnnounced {
			// cleanup if cleanup didn't finish before we restarted
			p.cleanupDealOnRestart(deal)
		}
	}

	// Restart active deals
	for _, deal := range activeDeals {

		// Set up a deal handler so that clients can subscribe to update
		// events about the deal
		dh, err := p.mkAndInsertDealHandler(deal.DealUuid)
		if err != nil {
			p.dealLogger.LogError(deal.DealUuid, "failed to restart deal", err)
			continue
		}

		// If it's an offline deal, and the deal data hasn't yet been
		// imported, just wait for the SP operator to import the data
		if deal.IsOffline && deal.InboundFilePath == "" {
			p.dealLogger.Infow(deal.DealUuid, "restarted deal: waiting for offline deal data import")
			continue
		}

		// Check if the deal can be restarted automatically.
		// Note that if the retry type is "fatal" then the deal should already
		// have been marked as complete (and therefore not returned by ListActive).
		if deal.Retry != smtypes.DealRetryAuto {
			p.dealLogger.Infow(deal.DealUuid, "deal must be manually restarted: waiting for manual restart")
			continue
		}

		// Restart deal
		p.dealLogger.Infow(deal.DealUuid, "resuming deal on boost restart", "checkpoint", deal.Checkpoint.String())
		_, err = p.startDealThread(dh, deal)
		if err != nil {
			p.dealLogger.LogError(deal.DealUuid, "failed to restart deal", err)
		}
	}

	// Start provider run loop
	go p.run()

	// Start sampling transfer data rate
	go p.transfers.start(p.ctx)

	log.Infow("storage provider: started")
	return nil
}

func (p *Provider) cleanupDealOnRestart(deal *types.ProviderDealState) {
	// remove the temp file created for inbound deal data if it is not an offline deal
	if !deal.IsOffline {
		_ = os.Remove(deal.InboundFilePath)
	}

	// untag storage space
	errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
	if errs == nil {
		p.dealLogger.Infow(deal.DealUuid, "untagged storage space")
	}
}

func (p *Provider) Stop() {
	p.closeSync.Do(func() {
		log.Infow("storage provider: shutdown")

		deals, err := p.dealsDB.ListActive(p.ctx)
		if err == nil {
			for i := range deals {
				dl := deals[i]
				if dl.Checkpoint < dealcheckpoints.AddedPiece {
					log.Infow("shutting down running deal", "id", dl.DealUuid.String(), "ckp", dl.Checkpoint.String())
				}
			}
		}

		log.Infow("storage provider: stop run loop")
		p.cancel()
		p.runWG.Wait()
		log.Info("storage provider: shutdown complete")
	})
}
