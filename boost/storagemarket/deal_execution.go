package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/application-research/estuary/boost/storagemarket/types"
	smtypes "github.com/application-research/estuary/boost/storagemarket/types"
	"github.com/application-research/estuary/boost/storagemarket/types/dealcheckpoints"
	"github.com/application-research/estuary/boost/transport"
	transporttypes "github.com/application-research/estuary/boost/transport/types"
	"github.com/filecoin-project/go-commp-utils/writer"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-state-types/abi"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p-core/event"
)

const (
	// DealCancelled means that a deal has been cancelled by the caller
	DealCancelled = "Cancelled"
)

type dealMakingError struct {
	error
	retry types.DealRetryType
}

func (p *Provider) runDeal(deal *types.ProviderDealState, dh *dealHandler) {
	// Log out the deal state at the start of execution (to help with debugging)
	dcpy := *deal
	dcpy.Transfer.Params = nil
	dcpy.ClientDealProposal.ClientSignature = acrypto.Signature{}
	p.dealLogger.Infow(deal.DealUuid, "deal execution initiated", "deal state", dcpy)

	// Clear any error from a previous run
	if deal.Err != "" || deal.Retry == smtypes.DealRetryAuto {
		deal.Err = ""
		deal.Retry = smtypes.DealRetryAuto
		p.saveDealToDB(dh.Publisher, deal)
	}

	// Execute the deal
	err := p.execDeal(deal, dh)
	if err == nil {
		// Deal completed successfully
		return
	}

	// If the error is fatal, fail the deal and cleanup resources (delete
	// downloaded data, untag funds etc)
	if err.retry == types.DealRetryFatal {
		cancelled := dh.TransferCancelledByUser()
		p.failDeal(dh.Publisher, deal, err, cancelled)
		return
	}

	// The error is recoverable, so just add a line to the deal log and
	// wait for the deal to be executed again (either manually by the user
	// or automatically when boost restarts)
	if errors.Is(err.error, context.Canceled) {
		p.dealLogger.Infow(deal.DealUuid, "deal paused because boost was shut down",
			"checkpoint", deal.Checkpoint.String())
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal paused because of recoverable error", "err", err.error.Error(),
			"checkpoint", deal.Checkpoint.String(), "retry", err.retry)
	}

	deal.Retry = err.retry
	deal.Err = err.Error()
	p.saveDealToDB(dh.Publisher, deal)
}

func (p *Provider) execDeal(deal *smtypes.ProviderDealState, dh *dealHandler) *dealMakingError {
	// If the deal has not yet been handed off to the sealer
	if deal.Checkpoint < dealcheckpoints.AddedPiece {
		transferType := "downloaded file"
		if deal.IsOffline {
			transferType = "imported offline deal file"
		}

		// Read the bytes received from the downloaded / imported file
		fi, err := os.Stat(deal.InboundFilePath)
		if err != nil {
			return &dealMakingError{
				error: fmt.Errorf("failed to get size of %s '%s': %w", transferType, deal.InboundFilePath, err),
				retry: smtypes.DealRetryFatal,
			}
		}
		deal.NBytesReceived = fi.Size()
		p.dealLogger.Infow(deal.DealUuid, "size of "+transferType, "filepath", deal.InboundFilePath, "size", fi.Size())
	} else {

		deal.NBytesReceived = int64(deal.Transfer.Size)
	}

	// Execute the deal synchronously
	if derr := p.execDealUptoAddPiece(dh.providerCtx, deal, dh); derr != nil {
		return derr
	}

	p.dealLogger.Infow(deal.DealUuid, "deal execution completed successfully")
	return nil
}

func (p *Provider) execDealUptoAddPiece(ctx context.Context, deal *types.ProviderDealState, dh *dealHandler) *dealMakingError {
	pub := dh.Publisher

	p.dealLogger.Infow(deal.DealUuid, "deal execution in progress")

	// Transfer Data step will be executed only if it's NOT an offline deal
	if !deal.IsOffline {
		if deal.Checkpoint < dealcheckpoints.Transferred {
			if err := p.transferAndVerify(dh.transferCtx, pub, deal); err != nil {
				// The transfer has failed. If the user tries to cancel the
				// transfer after this point it's a no-op.
				dh.setCancelTransferResponse(nil)

				// Pass through fatal errors
				if err.retry == smtypes.DealRetryFatal {
					return err
				}

				// If the transfer failed because the user cancelled the
				// transfer, it's non-recoverable
				if dh.TransferCancelledByUser() {
					return &dealMakingError{
						retry: types.DealRetryFatal,
						error: fmt.Errorf("data transfer cancelled after %d bytes: %w", deal.NBytesReceived, err),
					}
				}

				// If the transfer failed because boost was shut down, it's
				// automatically recoverable
				if errors.Is(err.error, context.Canceled) {
					return &dealMakingError{
						retry: types.DealRetryAuto,
						error: fmt.Errorf("data transfer paused by boost shutdown after %d bytes: %w", deal.NBytesReceived, err),
					}
				}

				// Pass through any other transfer failure
				return err
			}

			p.dealLogger.Infow(deal.DealUuid, "deal data transfer finished successfully")
		} else {
			p.dealLogger.Infow(deal.DealUuid, "deal data transfer has already been completed")
		}
		// transfer can no longer be cancelled
		dh.setCancelTransferResponse(errors.New("transfer already complete"))
		p.dealLogger.Infow(deal.DealUuid, "deal data-transfer can no longer be cancelled")
	} else if deal.Checkpoint < dealcheckpoints.Transferred {
		// verify CommP matches for an offline deal
		if err := p.verifyCommP(deal); err != nil {
			return &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("error when matching commP for imported data for offline deal: %w", err),
			}
		}
		p.dealLogger.Infow(deal.DealUuid, "commp matched successfully for imported data for offline deal")

		// update checkpoint
		if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.Transferred); derr != nil {
			return derr
		}
	}

	// as deal has already been handed to the sealer, we can remove the inbound file and reclaim the tagged space
	if !deal.IsOffline {
		// _ = os.Remove(deal.InboundFilePath)
		// p.dealLogger.Infow(deal.DealUuid, "removed inbound file as deal handed to sealer", "path", deal.InboundFilePath)
	}
	// if err := p.untagStorageSpaceAfterSealing(ctx, deal); err != nil {
	// 	// If there's an error untagging storage space we should still try to continue,
	// 	// so just log the error
	// 	p.dealLogger.Warnw(deal.DealUuid, "failed to untag storage space after handing deal to sealer", "err", err)
	// } else {
	// 	p.dealLogger.Infow(deal.DealUuid, "storage space successfully untagged for deal after it was handed to sealer")
	// }

	// Index deal in DAGStore and Announce deal
	if deal.Checkpoint < dealcheckpoints.IndexedAndAnnounced {
		p.dealLogger.Infow(deal.DealUuid, "deal successfully indexed and announced")
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal has already been indexed and announced")
	}

	p.Stop()

	return nil
}

func (p *Provider) untagStorageSpaceAfterSealing(ctx context.Context, deal *types.ProviderDealState) error {
	presp := make(chan struct{}, 1)
	select {
	case p.storageSpaceChan <- storageSpaceDealReq{deal: deal, done: presp}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-presp:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (p *Provider) transferAndVerify(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) *dealMakingError {
	p.dealLogger.Infow(deal.DealUuid, "transferring deal data", "transfer client id", deal.Transfer.ClientID)

	tctx, cancel := context.WithDeadline(ctx, time.Now().Add(time.Hour*24))
	defer cancel()

	st := time.Now()
	handler, err := p.Transport.Execute(tctx, deal.Transfer.Params, &transporttypes.TransportDealInfo{
		OutputFile: deal.InboundFilePath,
		DealUuid:   deal.DealUuid,
		DealSize:   int64(deal.Transfer.Size),
	})
	if err != nil {
		return &dealMakingError{
			retry: smtypes.DealRetryFatal,
			error: fmt.Errorf("transferAndVerify failed to start data transfer: %w", err),
		}
	}

	// wait for data-transfer to finish
	if err := p.waitForTransferFinish(tctx, handler, pub, deal); err != nil {
		// Note that the data transfer has automatic retries built in, so if
		// it fails, it means it's already retried several times and we should
		// surface the problem to the user so they can decide manually whether
		// to keep retrying
		return &dealMakingError{
			retry: smtypes.DealRetryManual,
			error: fmt.Errorf("data-transfer failed: %w", err),
		}
	}
	p.dealLogger.Infow(deal.DealUuid, "deal data-transfer completed successfully", "bytes received", deal.NBytesReceived, "time taken",
		time.Since(st).String())

	// Verify CommP matches
	if err := p.verifyCommP(deal); err != nil {
		return &dealMakingError{
			retry: smtypes.DealRetryFatal,
			error: fmt.Errorf("failed to verify CommP: %w", err),
		}
	}

	p.dealLogger.Infow(deal.DealUuid, "commP matched successfully: deal-data verified")
	return p.updateCheckpoint(pub, deal, dealcheckpoints.Transferred)
}

func (p *Provider) verifyCommP(deal *types.ProviderDealState) error {
	p.dealLogger.Infow(deal.DealUuid, "checking commP")
	pieceCid, err := p.GeneratePieceCommitment(deal.InboundFilePath, deal.ClientDealProposal.Proposal.PieceSize, deal.DealUuid)
	if err != nil {
		return fmt.Errorf("failed to generate CommP: %w", err)
	}

	clientPieceCid := deal.ClientDealProposal.Proposal.PieceCID
	if pieceCid != clientPieceCid {
		return fmt.Errorf("commP mismatch, expected=%s, actual=%s", clientPieceCid, pieceCid)
	}

	return nil
}

func (p *Provider) waitForTransferFinish(ctx context.Context, handler transport.Handler, pub event.Emitter, deal *types.ProviderDealState) error {
	defer handler.Close()
	defer p.transfers.complete(deal.DealUuid)
	var lastOutputPct int64

	logTransferProgress := func(received int64) {
		pct := (100 * received) / int64(deal.Transfer.Size)
		outputPct := pct / 10
		if outputPct != lastOutputPct {
			lastOutputPct = outputPct
			p.dealLogger.Infow(deal.DealUuid, "transfer progress", "bytes received", received,
				"deal size", deal.Transfer.Size, "percent complete", pct)
		}
	}

	for {
		select {
		case evt, ok := <-handler.Sub():
			if !ok {
				return nil
			}
			if evt.Error != nil {
				return evt.Error
			}
			deal.NBytesReceived = evt.NBytesReceived
			p.transfers.setBytes(deal.DealUuid, uint64(evt.NBytesReceived))
			p.fireEventDealUpdate(pub, deal)
			logTransferProgress(deal.NBytesReceived)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// GenerateCommP
func GenerateCommP(filepath string) (cidAndSize *writer.DataCIDSize, finalErr error) {
	rd, err := carv2.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get CARv2 reader: %w", err)
	}

	defer func() {
		if err := rd.Close(); err != nil {
			if finalErr == nil {
				cidAndSize = nil
				finalErr = fmt.Errorf("failed to close CARv2 reader: %w", err)
				return
			}
		}
	}()

	// dump the CARv1 payload of the CARv2 file to the Commp Writer and get back the CommP.
	w := &writer.Writer{}
	written, err := io.Copy(w, rd.DataReader())
	if err != nil {
		return nil, fmt.Errorf("failed to write to CommP writer: %w", err)
	}

	var size int64
	switch rd.Version {
	case 2:
		size = int64(rd.Header.DataSize)
	case 1:
		st, err := os.Stat(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat CARv1 file: %w", err)
		}
		size = st.Size()
	}

	if written != size {
		return nil, fmt.Errorf("number of bytes written to CommP writer %d not equal to the CARv1 payload size %d", written, rd.Header.DataSize)
	}

	cidAndSize = &writer.DataCIDSize{}
	*cidAndSize, err = w.Sum()
	if err != nil {
		return nil, fmt.Errorf("failed to get CommP: %w", err)
	}

	return cidAndSize, nil
}

// GeneratePieceCommitment generates the pieceCid for the CARv1 deal payload in
// the CARv2 file that already exists at the given path.
func (p *Provider) GeneratePieceCommitment(filepath string, dealSize abi.PaddedPieceSize, dealUuid uuid.UUID) (c cid.Cid, finalErr error) {
	cidAndSize, err := GenerateCommP(filepath)
	if err != nil {
		return cid.Undef, err
	}

	if cidAndSize.PieceSize < dealSize {
		p.dealLogger.Infow(dealUuid, "deal commp needs padding")
		// need to pad up!
		rawPaddedCommp, err := commp.PadCommP(
			// we know how long a pieceCid "hash" is, just blindly extract the trailing 32 bytes
			cidAndSize.PieceCID.Hash()[len(cidAndSize.PieceCID.Hash())-32:],
			uint64(cidAndSize.PieceSize),
			uint64(dealSize),
		)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to pad data: %w", err)
		}
		cidAndSize.PieceCID, _ = commcid.DataCommitmentV1ToCID(rawPaddedCommp)
	}
	return cidAndSize.PieceCID, err
}

func (p *Provider) failDeal(pub event.Emitter, deal *smtypes.ProviderDealState, err error, cancelled bool) {
	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = smtypes.DealRetryFatal
	if cancelled {
		deal.Err = DealCancelled
		p.dealLogger.Infow(deal.DealUuid, "deal cancelled by user")
	} else {
		deal.Err = err.Error()
		p.dealLogger.LogError(deal.DealUuid, "deal failed", err)
	}

	p.saveDealToDB(pub, deal)
	p.cleanupDeal(deal)
}

func (p *Provider) saveDealToDB(pub event.Emitter, deal *smtypes.ProviderDealState) {
	// In the case that the provider has been shutdown, the provider's context
	// will be cancelled, so use a background context when saving state to the
	// DB to avoid this edge case.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dberr := p.dealsDB.Update(ctx, deal)
	if dberr != nil {
		p.dealLogger.LogError(deal.DealUuid, "failed to update deal state in DB", dberr)
	}

	// Fire deal update event
	if pub != nil {
		p.fireEventDealUpdate(pub, deal)
	}
}

func (p *Provider) cleanupDeal(deal *types.ProviderDealState) {
	p.dealLogger.Infow(deal.DealUuid, "cleaning up deal")
	defer p.dealLogger.Infow(deal.DealUuid, "finished cleaning up deal")

	// remove the temp file created for inbound deal data if it is not an offline deal
	if !deal.IsOffline {
		_ = os.Remove(deal.InboundFilePath)
	}

	if deal.Checkpoint == dealcheckpoints.Complete {
		p.cleanupDealHandler(deal.DealUuid)
	}

	done := make(chan struct{}, 1)
	// submit req to event loop to untag tagged funds and storage space
	select {
	case p.finishedDealChan <- finishedDealReq{deal: deal, done: done}:
	case <-p.ctx.Done():
	}

	// wait for event loop to finish cleanup and return before we return from here
	// so caller is guaranteed that all resources associated with the deal have been cleanedup before
	// taking further action.
	select {
	case <-done:
	case <-p.ctx.Done():
	}
}

// cleanupDealHandler closes and cleans up the deal handler
func (p *Provider) cleanupDealHandler(dealUuid uuid.UUID) {
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return
	}

	dh.setCancelTransferResponse(errors.New("deal cleaned up"))
	dh.close()
	p.delDealHandler(dealUuid)
}

func (p *Provider) fireEventDealNew(deal *types.ProviderDealState) {
	if err := p.newDealPS.NewDeals.Emit(*deal); err != nil {
		p.dealLogger.Warnw(deal.DealUuid, "publishing new deal event", "err", err.Error())
	}
}

func (p *Provider) fireEventDealUpdate(pub event.Emitter, deal *types.ProviderDealState) {
	if err := pub.Emit(*deal); err != nil {
		p.dealLogger.Warnw(deal.DealUuid, "publishing deal state update", "err", err.Error())
	}
}

func (p *Provider) updateCheckpoint(pub event.Emitter, deal *types.ProviderDealState, ckpt dealcheckpoints.Checkpoint) *dealMakingError {
	prev := deal.Checkpoint
	deal.Checkpoint = ckpt
	deal.CheckpointAt = time.Now()
	// we don't want a graceful shutdown to mess with db updates so pass a background context
	if err := p.dealsDB.Update(context.Background(), deal); err != nil {
		return &dealMakingError{
			retry: smtypes.DealRetryFatal,
			error: fmt.Errorf("failed to persist deal state: %w", err),
		}
	}
	p.dealLogger.Infow(deal.DealUuid, "updated deal checkpoint in DB", "old checkpoint", prev.String(), "new checkpoint", ckpt.String())
	p.fireEventDealUpdate(pub, deal)

	return nil
}
