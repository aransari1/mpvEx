package xyz.mpv.rex.feature.webshare

import java.text.DecimalFormat

object WebShareHtmlTemplate {

  data class SharedFileItem(
    val id: String,
    val name: String,
    val size: Long,
    val formattedSize: String,
    val durationFormatted: String?,
    val mimeType: String,
  )

  fun renderHtml(
    files: List<SharedFileItem>,
    token: String? = null,
    serverTitle: String = "REX Player Web Share",
  ): String {
    val totalSize = files.sumOf { it.size }
    val totalSizeFormatted = formatFileSize(totalSize)
    val fileCount = files.size
    val multipleFiles = fileCount > 1
    val querySuffix = if (!token.isNullOrEmpty()) "?t=$token" else ""

    val fileCards = buildString {
      files.forEachIndexed { index, file ->
        val safeName = escapeHtml(file.name)
        val downloadUrl = "/download/${file.id}$querySuffix"
        val streamUrl = "/stream/${file.id}$querySuffix"

        append("""
          <div class="card">
            <div class="card-icon">
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5 3 19 12 5 21 5 3"></polygon>
              </svg>
            </div>
            <div class="card-content">
              <div class="card-title" title="$safeName" data-full-name="$safeName">$safeName</div>
              <div class="card-meta">
                <span>${file.formattedSize}</span>
                ${if (file.durationFormatted != null) """<span class="dot">•</span><span>${file.durationFormatted}</span>""" else ""}
              </div>
            </div>
            <div class="card-actions">
              <a href="$downloadUrl" class="btn btn-primary" download="$safeName">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                  <polyline points="7 10 12 15 17 10"></polyline>
                  <line x1="12" y1="15" x2="12" y2="3"></line>
                </svg>
                Download
              </a>
              <button class="btn btn-secondary" onclick="playMedia('$streamUrl', '$safeName')">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <circle cx="12" cy="12" r="10"></circle>
                  <polygon points="10 8 16 12 10 16 10 8"></polygon>
                </svg>
                Play
              </button>
            </div>
          </div>
        """.trimIndent())
      }
    }

    return """
      <!DOCTYPE html>
      <html lang="en">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>$serverTitle</title>
        <style>
          :root {
            --bg-color: #0f141c;
            --surface-color: #19202c;
            --surface-hover: #222b3a;
            --primary-color: #3b82f6;
            --primary-hover: #2563eb;
            --secondary-bg: #2d3748;
            --secondary-hover: #374151;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
            --border-color: #2e384d;
            --radius-lg: 16px;
            --radius-md: 10px;
            --shadow: 0 4px 20px rgba(0, 0, 0, 0.4);
          }

          @media (prefers-color-scheme: light) {
            :root {
              --bg-color: #f3f4f6;
              --surface-color: #ffffff;
              --surface-hover: #f9fafb;
              --primary-color: #2563eb;
              --primary-hover: #1d4ed8;
              --secondary-bg: #e5e7eb;
              --secondary-hover: #d1d5db;
              --text-main: #111827;
              --text-muted: #6b7280;
              --border-color: #e5e7eb;
              --shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
            }
          }

          * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
          }

          body {
            background-color: var(--bg-color);
            color: var(--text-main);
            min-height: 100vh;
            padding: 24px 16px 48px;
            display: flex;
            justify-content: center;
          }

          .container {
            width: 100%;
            max-width: 680px;
          }

          .header {
            text-align: center;
            margin-bottom: 28px;
          }

          .app-badge {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background-color: var(--surface-color);
            border: 1px solid var(--border-color);
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 13px;
            font-weight: 600;
            color: var(--primary-color);
            margin-bottom: 12px;
          }

          .title {
            font-size: 26px;
            font-weight: 800;
            letter-spacing: -0.5px;
            margin-bottom: 6px;
          }

          .subtitle {
            font-size: 14px;
            color: var(--text-muted);
          }

          .bulk-action {
            margin-bottom: 20px;
            text-align: right;
          }

          .card-list {
            display: flex;
            flex-direction: column;
            gap: 12px;
          }

          .card {
            background-color: var(--surface-color);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-lg);
            padding: 16px;
            display: flex;
            align-items: center;
            gap: 16px;
            box-shadow: var(--shadow);
            transition: transform 0.15s ease, background-color 0.15s ease;
          }

          .card:hover {
            background-color: var(--surface-hover);
          }

          .card-icon {
            width: 48px;
            height: 48px;
            border-radius: var(--radius-md);
            background-color: rgba(59, 130, 246, 0.15);
            color: var(--primary-color);
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
          }

          .card-content {
            flex: 1;
            min-width: 0;
          }

          .card-title {
            font-size: 15px;
            font-weight: 600;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            margin-bottom: 4px;
          }

          .card-meta {
            font-size: 13px;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 6px;
          }

          .dot {
            font-size: 10px;
          }

          .card-actions {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-shrink: 0;
          }

          .btn {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 14px;
            border-radius: var(--radius-md);
            font-size: 13px;
            font-weight: 600;
            text-decoration: none;
            border: none;
            cursor: pointer;
            transition: background-color 0.15s ease, transform 0.1s ease;
          }

          .btn:active {
            transform: scale(0.97);
          }

          .btn-primary {
            background-color: var(--primary-color);
            color: #ffffff;
          }

          .btn-primary:hover {
            background-color: var(--primary-hover);
          }

          .btn-secondary {
            background-color: var(--secondary-bg);
            color: var(--text-main);
          }

          .btn-secondary:hover {
            background-color: var(--secondary-hover);
          }

          /* Video Modal */
          .modal-backdrop {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            background: rgba(0, 0, 0, 0.85);
            backdrop-filter: blur(8px);
            z-index: 1000;
            align-items: center;
            justify-content: center;
            padding: 16px;
          }

          .modal-content {
            background: var(--surface-color);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-lg);
            width: 100%;
            max-width: 800px;
            overflow: hidden;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.6);
          }

          .modal-header {
            padding: 12px 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            border-bottom: 1px solid var(--border-color);
          }

          .modal-title {
            font-size: 15px;
            font-weight: 600;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            padding-right: 12px;
          }

          .modal-close {
            background: transparent;
            border: none;
            color: var(--text-muted);
            cursor: pointer;
            font-size: 24px;
            line-height: 1;
            padding: 4px;
          }

          .modal-close:hover {
            color: var(--text-main);
          }

          .modal-body {
            position: relative;
            background: #000;
            width: 100%;
            aspect-ratio: 16 / 9;
            display: flex;
            align-items: center;
            justify-content: center;
          }

          video {
            width: 100%;
            height: 100%;
          }

          .upload-section {
            margin-bottom: 24px;
          }

          .upload-card {
            background-color: var(--surface-color);
            border: 2px dashed var(--border-color);
            border-radius: var(--radius-lg);
            padding: 18px 20px;
            display: flex;
            align-items: center;
            gap: 16px;
            cursor: pointer;
            transition: all 0.2s ease;
            box-shadow: var(--shadow);
          }

          .upload-card:hover, .upload-card.drag-over {
            border-color: var(--primary-color);
            background-color: var(--surface-hover);
          }

          .upload-icon {
            width: 44px;
            height: 44px;
            border-radius: var(--radius-md);
            background-color: rgba(59, 130, 246, 0.12);
            color: var(--primary-color);
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
          }

          .upload-text {
            flex: 1;
            min-width: 0;
          }

          .upload-title {
            font-size: 15px;
            font-weight: 700;
            margin-bottom: 2px;
          }

          .upload-desc {
            font-size: 13px;
            color: var(--text-muted);
          }

          .upload-queue {
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-top: 12px;
          }

          .upload-item {
            background-color: var(--surface-color);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 12px 14px;
            box-shadow: var(--shadow);
          }

          .upload-item-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 13px;
            margin-bottom: 6px;
          }

          .upload-item-name {
            font-weight: 600;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 65%;
          }

          .upload-item-meta {
            color: var(--text-muted);
            font-size: 12px;
            font-weight: 500;
          }

          .upload-progress-bar {
            width: 100%;
            height: 6px;
            background-color: var(--secondary-bg);
            border-radius: 9999px;
            overflow: hidden;
          }

          .upload-progress-fill {
            height: 100%;
            background-color: var(--primary-color);
            border-radius: 9999px;
            width: 0%;
            transition: width 0.15s ease;
          }

          .upload-item.success .upload-progress-fill {
            background-color: #10b981;
          }

          .upload-item.error .upload-progress-fill {
            background-color: #ef4444;
          }

          @media (max-width: 480px) {
            .card {
              flex-direction: column;
              align-items: flex-start;
              gap: 12px;
            }
            .card-content {
              width: 100%;
            }
            .card-actions {
              width: 100%;
              justify-content: flex-end;
            }
            .btn {
              flex: 1;
              justify-content: center;
            }
          }
        </style>
      </head>
      <body>
        <div class="container">
          <header class="header">
            <div class="app-badge">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"></circle>
                <polygon points="10 8 16 12 10 16 10 8"></polygon>
              </svg>
              REX Player Share
            </div>
            <h1 class="title">Shared Files</h1>
            <p class="subtitle">$fileCount ${if (multipleFiles) "files" else "file"} • $totalSizeFormatted total</p>
          </header>

          <!-- Send Files to Host Dropzone -->
          <section class="upload-section">
            <div class="upload-card" id="dropZone" onclick="document.getElementById('fileInput').click()">
              <input type="file" id="fileInput" multiple style="display:none" onchange="handleFileSelect(this.files)">
              <div class="upload-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                  <polyline points="17 8 12 3 7 8"></polyline>
                  <line x1="12" y1="3" x2="12" y2="15"></line>
                </svg>
              </div>
              <div class="upload-text">
                <div class="upload-title">Send Files to REX Player</div>
                <div class="upload-desc">Tap to browse or drop files to send</div>
              </div>
            </div>
            <div id="uploadQueue" class="upload-queue"></div>
          </section>

          ${if (multipleFiles) """
            <div class="bulk-action">
              <a href="/download-all$querySuffix" class="btn btn-primary" download="REX_Player_shared_files.zip">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <polyline points="21 8 21 21 3 21 3 8"></polyline>
                  <rect x="1" y="3" width="22" height="5"></rect>
                  <line x1="10" y1="12" x2="14" y2="12"></line>
                </svg>
                Download All (ZIP)
              </a>
            </div>
          """ else ""}

          <main class="card-list">
            $fileCards
          </main>
        </div>

        <!-- Media Player Modal -->
        <div id="mediaModal" class="modal-backdrop" onclick="if(event.target === this) closePlayer()">
          <div class="modal-content">
            <div class="modal-header">
              <div id="modalTitle" class="modal-title"></div>
              <button class="modal-close" onclick="closePlayer()">&times;</button>
            </div>
            <div class="modal-body">
              <video id="mediaPlayer" controls autoplay playsinline preload="metadata"></video>
            </div>
          </div>
        </div>

        <script>
          function truncateTitle(el) {
            const fullName = el.getAttribute('data-full-name') || el.textContent;
            if (!fullName) return;
            if (!el.hasAttribute('data-full-name')) {
              el.setAttribute('data-full-name', fullName);
            }

            // Always reset to full original filename first to measure unclipped dimensions
            el.textContent = fullName;

            // Strict check: if filename fits without overflowing, never shorten it!
            if (el.scrollWidth <= el.clientWidth) {
              return;
            }

            const dotIdx = fullName.lastIndexOf('.');
            const hasExt = dotIdx > 0 && dotIdx < fullName.length - 1;
            const ext = hasExt ? fullName.substring(dotIdx) : '';
            const base = hasExt ? fullName.substring(0, dotIdx) : fullName;

            // Preserve last 4 characters before the extension (e.g. "...this.apk")
            const tailLen = Math.min(4, Math.max(0, Math.floor(base.length / 2) - 1));
            const tail = tailLen > 0 ? base.slice(-tailLen) : '';
            const head = tailLen > 0 ? base.slice(0, -tailLen) : base;
            const suffix = '...' + tail + ext;

            let low = 1;
            let high = head.length;
            let best = 0;

            while (low <= high) {
              const mid = Math.floor((low + high) / 2);
              el.textContent = head.substring(0, mid) + suffix;
              if (el.scrollWidth <= el.clientWidth) {
                best = mid;
                low = mid + 1;
              } else {
                high = mid - 1;
              }
            }

            if (best > 0) {
              el.textContent = head.substring(0, best) + suffix;
            } else {
              // Fallback to head + '...' + ext if suffix was too long
              const simpleSuffix = '...' + ext;
              low = 1;
              high = base.length - 1;
              best = 1;
              while (low <= high) {
                const mid = Math.floor((low + high) / 2);
                el.textContent = base.substring(0, mid) + simpleSuffix;
                if (el.scrollWidth <= el.clientWidth) {
                  best = mid;
                  low = mid + 1;
                } else {
                  high = mid - 1;
                }
              }
              el.textContent = base.substring(0, best) + simpleSuffix;
            }
          }

          function truncateAllTitles() {
            document.querySelectorAll('.card-title').forEach(truncateTitle);
            const modalTitle = document.getElementById('modalTitle');
            if (modalTitle && modalTitle.textContent) {
              truncateTitle(modalTitle);
            }
          }

          function playMedia(streamUrl, title) {
            const modal = document.getElementById('mediaModal');
            const player = document.getElementById('mediaPlayer');
            const titleEl = document.getElementById('modalTitle');
            titleEl.setAttribute('data-full-name', title);
            titleEl.textContent = title;
            player.src = streamUrl;
            modal.style.display = 'flex';
            truncateTitle(titleEl);
            player.play().catch(() => {});
          }

          function closePlayer() {
            const modal = document.getElementById('mediaModal');
            const player = document.getElementById('mediaPlayer');
            player.pause();
            player.removeAttribute('src');
            player.load();
            modal.style.display = 'none';
          }

          document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') closePlayer();
          });

          // Upload Controller
          const tokenQuery = '$querySuffix';

          function handleFileSelect(files) {
            if (!files || files.length === 0) return;
            const queue = Array.from(files);
            uploadFilesSequential(queue);
            const input = document.getElementById('fileInput');
            if (input) input.value = '';
          }

          function uploadFilesSequential(queue) {
            if (queue.length === 0) return;
            const file = queue.shift();
            uploadSingleFile(file, () => {
              uploadFilesSequential(queue);
            });
          }

          function uploadSingleFile(file, onComplete) {
            const queueContainer = document.getElementById('uploadQueue');
            const itemId = 'upload-' + Math.random().toString(36).substring(2, 9);
            const safeName = escapeHtmlText(file.name);

            const itemEl = document.createElement('div');
            itemEl.className = 'upload-item';
            itemEl.id = itemId;
            itemEl.innerHTML = '<div class="upload-item-header"><div class="upload-item-name" title="' + safeName + '">' + safeName + '</div><div class="upload-item-meta" id="' + itemId + '-status">0%</div></div><div class="upload-progress-bar"><div class="upload-progress-fill" id="' + itemId + '-fill"></div></div>';
            queueContainer.prepend(itemEl);

            const xhr = new XMLHttpRequest();
            const url = '/upload?name=' + encodeURIComponent(file.name) + (tokenQuery ? (tokenQuery.startsWith('?') ? '&' + tokenQuery.substring(1) : tokenQuery) : '');
            let startTime = Date.now();

            xhr.upload.onprogress = function(e) {
              if (e.lengthComputable) {
                const percent = Math.round((e.loaded / e.total) * 100);
                const fillEl = document.getElementById(itemId + '-fill');
                const statusEl = document.getElementById(itemId + '-status');
                if (fillEl) fillEl.style.width = percent + '%';
                const elapsedSec = (Date.now() - startTime) / 1000;
                if (elapsedSec > 0.5) {
                  const speedMB = ((e.loaded / (1024 * 1024)) / elapsedSec).toFixed(1);
                  if (statusEl) statusEl.textContent = percent + '% • ' + speedMB + ' MB/s';
                } else {
                  if (statusEl) statusEl.textContent = percent + '%';
                }
              }
            };

            xhr.onload = function() {
              const statusEl = document.getElementById(itemId + '-status');
              const fillEl = document.getElementById(itemId + '-fill');
              if (xhr.status >= 200 && xhr.status < 300) {
                itemEl.classList.add('success');
                if (fillEl) fillEl.style.width = '100%';
                if (statusEl) statusEl.textContent = '✓ Saved';
              } else {
                itemEl.classList.add('error');
                if (statusEl) statusEl.textContent = '✗ Upload failed';
              }
              if (onComplete) onComplete();
            };

            xhr.onerror = function() {
              const statusEl = document.getElementById(itemId + '-status');
              itemEl.classList.add('error');
              if (statusEl) statusEl.textContent = '✗ Connection error';
              if (onComplete) onComplete();
            };

            xhr.open('POST', url, true);
            xhr.send(file);
          }

          function escapeHtmlText(text) {
            return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
          }

          // Drag and drop listeners
          const dropZone = document.getElementById('dropZone');
          if (dropZone) {
            ['dragenter', 'dragover'].forEach(eventName => {
              dropZone.addEventListener(eventName, (e) => {
                e.preventDefault();
                e.stopPropagation();
                dropZone.classList.add('drag-over');
              }, false);
            });

            ['dragleave', 'drop'].forEach(eventName => {
              dropZone.addEventListener(eventName, (e) => {
                e.preventDefault();
                e.stopPropagation();
                dropZone.classList.remove('drag-over');
              }, false);
            });

            dropZone.addEventListener('drop', (e) => {
              const dt = e.dataTransfer;
              if (dt && dt.files) {
                handleFileSelect(dt.files);
              }
            }, false);
          }

          window.addEventListener('load', truncateAllTitles);
          window.addEventListener('resize', truncateAllTitles);
          if (document.fonts && document.fonts.ready) {
            document.fonts.ready.then(truncateAllTitles);
          }
          truncateAllTitles();
        </script>
      </body>
      </html>
    """.trimIndent()
  }

  private fun formatFileSize(size: Long): String {
    if (size <= 0) return "0 B"
    val units = arrayOf("B", "KB", "MB", "GB", "TB")
    val digitGroups = (Math.log10(size.toDouble()) / Math.log10(1024.0)).toInt().coerceIn(0, units.size - 1)
    return DecimalFormat("#,##0.#").format(size / Math.pow(1024.0, digitGroups.toDouble())) + " " + units[digitGroups]
  }

  private fun escapeHtml(text: String): String {
    return text.replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")
  }
}
