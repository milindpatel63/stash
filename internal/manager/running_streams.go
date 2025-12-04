package manager

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	"github.com/stashapp/stash/internal/manager/config"
	"github.com/stashapp/stash/internal/static"
	"github.com/stashapp/stash/pkg/ffmpeg"
	"github.com/stashapp/stash/pkg/fsutil"
	"github.com/stashapp/stash/pkg/logger"
	"github.com/stashapp/stash/pkg/models"
	"github.com/stashapp/stash/pkg/txn"
	"github.com/stashapp/stash/pkg/utils"
)

func KillRunningStreams(scene *models.Scene, fileNamingAlgo models.HashAlgorithm) {
	instance.ReadLockManager.Cancel(scene.Path)

	sceneHash := scene.GetHash(fileNamingAlgo)

	if sceneHash == "" {
		return
	}

	transcodePath := GetInstance().Paths.Scene.GetTranscodePath(sceneHash)
	instance.ReadLockManager.Cancel(transcodePath)
}

type SceneCoverGetter interface {
	GetCover(ctx context.Context, sceneID int) ([]byte, error)
}

type SceneServer struct {
	TxnManager       txn.Manager
	SceneCoverGetter SceneCoverGetter
}

func (s *SceneServer) StreamSceneDirect(scene *models.Scene, w http.ResponseWriter, r *http.Request) {
	// #3526 - return 404 if the scene does not have any files
	if scene.Path == "" {
		http.Error(w, http.StatusText(404), 404)
		return
	}

	algo := config.GetInstance().GetVideoFileNamingAlgorithm()

	// Cancel any existing streams for this scene before starting a new one.
	// This ensures that only the most recent request (from Stash UI or external apps)
	// is actively reading from disk.
	KillRunningStreams(scene, algo)

	sceneHash := scene.GetHash(algo)

	fp := GetInstance().Paths.Scene.GetStreamPath(scene.Path, sceneHash)
	streamRequestCtx := ffmpeg.NewStreamRequestContext(w, r)

	// #2579 - hijacking and closing the connection here causes video playback to fail in Safari
	// We trust that the request context will be closed, so we don't need to call Cancel on the
	// returned context here.
	// Use the original scene.Path as the read lock key so KillRunningStreams can cancel
	// any existing direct streams for this scene.
	_ = GetInstance().ReadLockManager.ReadLock(streamRequestCtx, scene.Path)
	_, filename := filepath.Split(fp)
	contentDisposition := mime.FormatMediaType("inline", map[string]string{"filename": filename})
	w.Header().Set("Content-Disposition", contentDisposition)

	// IO-level limiting: ensure at most N concurrent open file handles per file path.
	// This applies regardless of who calls /scene/:id/stream (Stash UI or external apps).
	if limiter := GetInstance().IOLimiter; limiter != nil {
		limiter.Acquire(fp)
		defer limiter.Release(fp)
	}

	// Manually open the file so we control when the handle is acquired and released.
	f, err := os.Open(fp)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Use ServeContent so HTTP Range requests are handled correctly on a single file handle.
	http.ServeContent(w, r, filename, fi.ModTime(), f)
}

func (s *SceneServer) ServeScreenshot(scene *models.Scene, w http.ResponseWriter, r *http.Request) {
	var cover []byte
	readTxnErr := txn.WithReadTxn(r.Context(), s.TxnManager, func(ctx context.Context) error {
		var err error
		cover, err = s.SceneCoverGetter.GetCover(ctx, scene.ID)
		return err
	})
	if errors.Is(readTxnErr, context.Canceled) {
		return
	}
	if readTxnErr != nil {
		logger.Warnf("read transaction error on fetch screenshot: %v", readTxnErr)
	}

	if cover == nil {
		// fallback to legacy image if present
		if scene.Path != "" {
			sceneHash := scene.GetHash(config.GetInstance().GetVideoFileNamingAlgorithm())
			filepath := GetInstance().Paths.Scene.GetLegacyScreenshotPath(sceneHash)

			// fall back to the scene image blob if the file isn't present
			screenshotExists, _ := fsutil.FileExists(filepath)
			if screenshotExists {
				if r.URL.Query().Has("t") {
					w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
				} else {
					w.Header().Set("Cache-Control", "no-cache")
				}
				http.ServeFile(w, r, filepath)
				return
			}
		}

		// fallback to default cover if none found
		cover = static.ReadAll(static.DefaultSceneImage)
	}

	utils.ServeImage(w, r, cover)
}
