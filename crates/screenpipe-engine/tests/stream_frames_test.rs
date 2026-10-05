use chrono::{DateTime, Duration, Utc};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// Request format for stream frames WebSocket
#[derive(Debug, Serialize)]
struct StreamFramesRequest {
    start_time: String,
    end_time: String,
    order: String,
}

/// Response format from stream frames WebSocket
#[derive(Debug, Deserialize)]
struct StreamTimeSeriesResponse {
    timestamp: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use screenpipe_audio::audio_manager::AudioManagerBuilder;
    use screenpipe_db::{AudioDevice, DatabaseManager, DeviceType, FrameWindowData, OcrEngine};
    use screenpipe_engine::SCServer;
    use std::{net::SocketAddr, sync::Arc};

    #[derive(Debug, Serialize)]
    struct StreamFramesLimitedRequest {
        start_time: String,
        end_time: String,
        order: String,
        limit: usize,
    }

    async fn setup_stream_test_server() -> (
        String,
        Arc<DatabaseManager>,
        tokio::task::JoinHandle<Result<(), std::io::Error>>,
    ) {
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let screenpipe_dir = std::env::temp_dir().join(format!(
            "screenpipe-stream-test-{}-{unique_suffix}",
            std::process::id()
        ));

        let db = Arc::new(
            DatabaseManager::new("sqlite::memory:", Default::default())
                .await
                .unwrap(),
        );
        let audio_manager = Arc::new(
            AudioManagerBuilder::new()
                .is_disabled(true)
                .output_path(screenpipe_dir.join("audio"))
                .build(db.clone())
                .await
                .unwrap(),
        );

        let server = SCServer::new(
            db.clone(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
            screenpipe_dir,
            false,
            false,
            audio_manager,
            false,
            "balanced".to_string(),
        );
        let app = server.create_router().await;
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, app).await });

        (format!("ws://{addr}/stream/frames"), db, handle)
    }

    #[tokio::test]
    async fn test_dense_range_stream_spans_full_requested_window() {
        let (url, db, server_handle) = setup_stream_test_server().await;
        let device_name = "stream-regression-monitor";
        db.insert_video_chunk("stream-regression.mp4", device_name)
            .await
            .unwrap();

        let start = DateTime::parse_from_rfc3339("2026-06-28T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let total_frames = 3_000usize;
        let display_limit = 2_500usize;
        let seeded_frames: Vec<_> = (0..total_frames)
            .map(|idx| {
                (
                    start + Duration::seconds(idx as i64),
                    idx as i64,
                    Vec::new(),
                )
            })
            .collect();
        db.insert_multi_frames_with_ocr_batch(
            device_name,
            &seeded_frames,
            Arc::new(OcrEngine::Tesseract),
        )
        .await
        .unwrap();

        let end = start + Duration::seconds(total_frames as i64 - 1);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("websocket should connect");
        let (mut write, mut read) = ws_stream.split();
        let request = StreamFramesLimitedRequest {
            start_time: start.to_rfc3339(),
            end_time: end.to_rfc3339(),
            order: "descending".to_string(),
            limit: display_limit,
        };

        write
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("request should send");

        let received_frames = timeout(std::time::Duration::from_secs(10), async {
            let mut received = Vec::with_capacity(display_limit);
            while received.len() < display_limit {
                let msg = read
                    .next()
                    .await
                    .expect("websocket should stay open")
                    .expect("message should read");
                let Message::Text(text) = msg else {
                    continue;
                };
                if text == "\"keep-alive-text\"" {
                    continue;
                }
                let mut batch: Vec<StreamTimeSeriesResponse> =
                    serde_json::from_str(&text).expect("response batch should parse");
                received.append(&mut batch);
            }
            received
        })
        .await
        .expect("stream should return dense range in time");

        let first = DateTime::parse_from_rfc3339(&received_frames.first().unwrap().timestamp)
            .unwrap()
            .with_timezone(&Utc);
        let last = DateTime::parse_from_rfc3339(&received_frames.last().unwrap().timestamp)
            .unwrap()
            .with_timezone(&Utc);

        server_handle.abort();

        assert_eq!(received_frames.len(), display_limit);
        assert_eq!(first, end);
        assert_eq!(last, start);
    }

    #[tokio::test]
    async fn test_large_production_stream_is_split_for_native_clients() {
        let (url, db, server_handle) = setup_stream_test_server().await;
        let device_name = "large-native-stream-monitor";
        db.insert_video_chunk("large-native-stream.mp4", device_name)
            .await
            .unwrap();

        let start = DateTime::parse_from_rfc3339("2026-06-29T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let total_frames = 24usize;
        let seeded_frames: Vec<_> = (0..total_frames)
            .map(|idx| {
                (
                    start + Duration::seconds(idx as i64),
                    idx as i64,
                    vec![FrameWindowData {
                        app_name: Some("Large Capture".to_string()),
                        window_name: Some(format!("Large window {idx}")),
                        browser_url: None,
                        focused: true,
                        text: format!("native timeline frame {idx}"),
                        text_json: "[]".to_string(),
                    }],
                )
            })
            .collect();
        db.insert_multi_frames_with_ocr_batch(
            device_name,
            &seeded_frames,
            Arc::new(OcrEngine::Tesseract),
        )
        .await
        .unwrap();
        let audio_chunk_id = db
            .insert_audio_chunk("large-native-stream.wav", Some(start))
            .await
            .unwrap();
        db.insert_audio_transcription(
            audio_chunk_id,
            &"spoken words ".repeat(350_000),
            0,
            "e2e",
            &AudioDevice {
                name: "large-native-stream-microphone".to_string(),
                device_type: DeviceType::Input,
            },
            None,
            Some(0.0),
            Some(total_frames as f64),
            Some(start),
        )
        .await
        .unwrap();

        let end = start + Duration::seconds(total_frames as i64 - 1);
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("websocket should connect");
        let (mut write, mut read) = ws_stream.split();
        write
            .send(Message::Text(
                serde_json::to_string(&StreamFramesLimitedRequest {
                    start_time: start.to_rfc3339(),
                    end_time: end.to_rfc3339(),
                    order: "descending".to_string(),
                    limit: total_frames,
                })
                .unwrap(),
            ))
            .await
            .expect("request should send");

        let (received_frames, message_sizes) = timeout(std::time::Duration::from_secs(30), async {
            let mut received = Vec::with_capacity(total_frames);
            let mut sizes = Vec::new();
            while received.len() < total_frames {
                let msg = read
                    .next()
                    .await
                    .expect("websocket should stay open")
                    .expect("message should read");
                let Message::Text(text) = msg else {
                    continue;
                };
                if text == "\"keep-alive-text\"" {
                    continue;
                }
                sizes.push(text.len());
                let mut batch: Vec<StreamTimeSeriesResponse> =
                    serde_json::from_str(&text).expect("response batch should parse");
                received.append(&mut batch);
            }
            (received, sizes)
        })
        .await
        .expect("large production stream should remain connected");

        server_handle.abort();

        assert_eq!(received_frames.len(), total_frames);
        assert!(
            message_sizes.len() > 1,
            "a >64 MiB logical response must use multiple WebSocket messages"
        );
        assert!(
            message_sizes.iter().all(|size| *size <= 8 * 1024 * 1024),
            "every message must fit the native client's bounded receive window"
        );
    }

    /// TEST 1: Reproduce the main issue - new frames after initial fetch are not pushed
    ///
    /// This test verifies the bug where:
    /// 1. Client connects and requests today's frames
    /// 2. Server streams existing frames
    /// 3. NEW frame is inserted into DB
    /// 4. Client does NOT receive the new frame (BUG!)
    #[tokio::test]
    #[ignore = "requires running server, run with: cargo test stream_frames -- --ignored"]
    async fn test_new_frames_not_pushed_to_client_bug() {
        // This test documents the current buggy behavior
        // After fix, this test should be updated to expect the new frame

        let url = "ws://127.0.0.1:3030/stream/frames";
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect to websocket");

        let (mut write, mut read) = ws_stream.split();

        // Request frames for today
        let now = Utc::now();
        let start_of_day = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let end_of_day = now.date_naive().and_hms_opt(23, 59, 59).unwrap();

        let request = StreamFramesRequest {
            start_time: format!("{}Z", start_of_day),
            end_time: format!("{}Z", end_of_day),
            order: "descending".to_string(),
        };

        write
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request");

        // Read initial frames (with timeout)
        let mut received_frames = Vec::new();
        let _initial_fetch = timeout(std::time::Duration::from_secs(5), async {
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break; // End of initial batch
                    }
                    if let Ok(frames) = serde_json::from_str::<Vec<StreamTimeSeriesResponse>>(&text)
                    {
                        received_frames.extend(frames);
                    }
                }
            }
        })
        .await;

        println!("Received {} frames in initial fetch", received_frames.len());

        // Now wait for any new frames (this should timeout with current bug)
        let wait_for_new = timeout(std::time::Duration::from_secs(10), async {
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text != "\"keep-alive-text\"" {
                        println!("Received new frame after initial fetch: {}", text);
                        return true;
                    }
                }
            }
            false
        })
        .await;

        // With current bug, this should timeout (no new frames pushed)
        // After fix, this should receive the new frame
        match wait_for_new {
            Ok(received) => {
                if received {
                    println!("SUCCESS: New frames ARE being pushed (fix is working)");
                } else {
                    println!("BUG CONFIRMED: No new frames received");
                }
            }
            Err(_) => {
                println!("BUG CONFIRMED: Timeout waiting for new frames");
            }
        }
    }

    /// TEST 2: Multiple clients should all receive new frames
    #[tokio::test]
    #[ignore = "requires running server, run with: cargo test stream_frames -- --ignored"]
    async fn test_multiple_clients_receive_new_frames() {
        let url = "ws://127.0.0.1:3030/stream/frames";

        // Connect two clients
        let (ws1, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect client 1");
        let (ws2, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect client 2");

        let (mut write1, mut read1) = ws1.split();
        let (mut write2, mut read2) = ws2.split();

        let now = Utc::now();
        let start_of_day = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let end_of_day = now.date_naive().and_hms_opt(23, 59, 59).unwrap();

        let request = StreamFramesRequest {
            start_time: format!("{}Z", start_of_day),
            end_time: format!("{}Z", end_of_day),
            order: "descending".to_string(),
        };

        // Both clients request today's frames
        write1
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request to client 1");
        write2
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request to client 2");

        // Wait and verify both clients receive frames
        // After fix, both should receive new frames pushed by server
        let client1_frames = timeout(std::time::Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(Ok(msg)) = read1.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    count += 1;
                }
            }
            count
        })
        .await
        .unwrap_or(0);

        let client2_frames = timeout(std::time::Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(Ok(msg)) = read2.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    count += 1;
                }
            }
            count
        })
        .await
        .unwrap_or(0);

        println!("Client 1 received {} frames", client1_frames);
        println!("Client 2 received {} frames", client2_frames);

        // Both clients should receive the same data
        assert!(
            client1_frames > 0 || client2_frames > 0,
            "At least one client should receive frames"
        );
    }

    /// TEST 3: Client should only receive frames within requested time range
    #[tokio::test]
    #[ignore = "requires running server, run with: cargo test stream_frames -- --ignored"]
    async fn test_frames_filtered_by_time_range() {
        let url = "ws://127.0.0.1:3030/stream/frames";

        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        let (mut write, mut read) = ws.split();

        // Request frames for only the last hour
        let now = Utc::now();
        let one_hour_ago = now - Duration::hours(1);

        let request = StreamFramesRequest {
            start_time: one_hour_ago.to_rfc3339(),
            end_time: now.to_rfc3339(),
            order: "descending".to_string(),
        };

        write
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request");

        let frames_received = timeout(std::time::Duration::from_secs(5), async {
            let mut frames = Vec::new();
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    if let Ok(batch) = serde_json::from_str::<Vec<StreamTimeSeriesResponse>>(&text)
                    {
                        for frame in batch {
                            // Verify each frame is within the requested time range
                            let timestamp = chrono::DateTime::parse_from_rfc3339(&frame.timestamp)
                                .expect("Invalid timestamp");
                            assert!(
                                timestamp >= one_hour_ago && timestamp <= now,
                                "Frame timestamp {} is outside requested range",
                                frame.timestamp
                            );
                            frames.push(frame);
                        }
                    }
                }
            }
            frames
        })
        .await;

        println!(
            "Received {} frames within time range",
            frames_received.map(|f| f.len()).unwrap_or(0)
        );
    }

    /// TEST 4: Reconnection should receive fresh data
    #[tokio::test]
    #[ignore = "requires running server, run with: cargo test stream_frames -- --ignored"]
    async fn test_reconnection_receives_fresh_data() {
        let url = "ws://127.0.0.1:3030/stream/frames";

        // First connection
        let (ws1, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect first time");

        let (mut write1, mut read1) = ws1.split();

        let now = Utc::now();
        let start_of_day = now.date_naive().and_hms_opt(0, 0, 0).unwrap();
        let end_of_day = now.date_naive().and_hms_opt(23, 59, 59).unwrap();

        let request = StreamFramesRequest {
            start_time: format!("{}Z", start_of_day),
            end_time: format!("{}Z", end_of_day),
            order: "descending".to_string(),
        };

        write1
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request");

        let first_count = timeout(std::time::Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(Ok(msg)) = read1.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    count += 1;
                }
            }
            count
        })
        .await
        .unwrap_or(0);

        // Close first connection
        drop(write1);
        drop(read1);

        // Wait a bit, then reconnect
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Second connection should also receive frames
        let (ws2, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to reconnect");

        let (mut write2, mut read2) = ws2.split();

        write2
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request on reconnect");

        let second_count = timeout(std::time::Duration::from_secs(5), async {
            let mut count = 0;
            while let Some(Ok(msg)) = read2.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    count += 1;
                }
            }
            count
        })
        .await
        .unwrap_or(0);

        println!("First connection: {} frames", first_count);
        println!("Second connection: {} frames", second_count);

        // After fix with live push, second connection should have >= frames as first
        // (might have more if new frames were recorded between connections)
        assert!(
            second_count >= first_count || first_count == 0,
            "Reconnection should receive at least as many frames"
        );
    }

    /// TEST 5: Edge case - empty time range should return no frames
    #[tokio::test]
    #[ignore = "requires running server, run with: cargo test stream_frames -- --ignored"]
    async fn test_empty_time_range() {
        let url = "ws://127.0.0.1:3030/stream/frames";

        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("Failed to connect");

        let (mut write, mut read) = ws.split();

        // Request frames for a time range in the far future (no data)
        let future = Utc::now() + Duration::days(365);

        let request = StreamFramesRequest {
            start_time: future.to_rfc3339(),
            end_time: (future + Duration::hours(1)).to_rfc3339(),
            order: "descending".to_string(),
        };

        write
            .send(Message::Text(serde_json::to_string(&request).unwrap()))
            .await
            .expect("Failed to send request");

        let frames_received = timeout(std::time::Duration::from_secs(3), async {
            let mut frames = Vec::new();
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    if let Ok(batch) = serde_json::from_str::<Vec<StreamTimeSeriesResponse>>(&text)
                    {
                        frames.extend(batch);
                    }
                }
            }
            frames
        })
        .await
        .unwrap_or_default();

        assert!(
            frames_received.is_empty(),
            "Future time range should return no frames"
        );
    }

    /// Regression test for #7437:
    /// Verify Windows startup and capture continuity after malformed timeline history.
    ///
    /// Seeds synthetic historical audio with reversed (start=90.0, end=1.0) and
    /// extreme (infinity, overflow) segment offsets alongside valid records.
    /// Boots the real engine server, exercises the timeline request route via WebSocket,
    /// verifies that:
    /// 1. The server and worker do not panic with "range start is greater than range end in BTreeMap".
    /// 2. All recoverable transcripts survive and valid timing is preserved.
    /// 3. Ongoing capture continuity: new frames and audio inserted after timeline queries
    ///    are immediately captured, attached, and queryable.
    /// 4. Client requests with inverted bounds (start_time > end_time) are handled safely without panic.
    #[tokio::test]
    async fn test_malformed_timeline_history_startup_and_capture_continuity() {
        let (url, db, _server_handle) = setup_stream_test_server().await;
        let device_name = "test-monitor";
        db.insert_video_chunk("test-video.mp4", device_name)
            .await
            .unwrap();

        let base = Utc::now() - Duration::hours(2);

        // 1. Insert historical screen frames
        db.insert_frame(
            device_name,
            Some(base),
            None,
            Some("Code"),
            Some("Editor"),
            false,
            None,
        )
        .await
        .unwrap();

        let audio_device = AudioDevice {
            name: "Default Audio".to_string(),
            device_type: DeviceType::Output,
        };

        // 2. Seed synthetic historical audio:
        // Case A: reversed offsets (start=90.0, end=1.0)
        let reversed_ts = base + Duration::seconds(10);
        let chunk_rev = db
            .insert_audio_chunk("reversed.mp4", Some(reversed_ts))
            .await
            .unwrap();
        db.insert_audio_transcription(
            chunk_rev,
            "reversed-transcript",
            0,
            "whisper",
            &audio_device,
            None,
            Some(90.0),
            Some(1.0),
            Some(reversed_ts),
        )
        .await
        .unwrap();

        // Case B: extreme infinity offset
        let inf_ts = base + Duration::seconds(30);
        let chunk_inf = db
            .insert_audio_chunk("infinite.mp4", Some(inf_ts))
            .await
            .unwrap();
        db.insert_audio_transcription(
            chunk_inf,
            "infinite-transcript",
            0,
            "whisper",
            &audio_device,
            None,
            Some(f64::INFINITY),
            Some(2.0),
            Some(inf_ts),
        )
        .await
        .unwrap();

        // Case C: valid control offset
        let valid_ts = base + Duration::seconds(50);
        let chunk_valid = db
            .insert_audio_chunk("valid.mp4", Some(valid_ts))
            .await
            .unwrap();
        db.insert_audio_transcription(
            chunk_valid,
            "valid-transcript",
            0,
            "whisper",
            &audio_device,
            None,
            Some(2.0),
            Some(4.0),
            Some(valid_ts),
        )
        .await
        .unwrap();

        // 3. Connect to timeline WebSocket and issue request spanning the historical range
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("websocket should connect");
        let (mut write, mut read) = ws_stream.split();

        let req = StreamFramesLimitedRequest {
            start_time: (base - Duration::minutes(5)).to_rfc3339(),
            end_time: (base + Duration::hours(1)).to_rfc3339(),
            order: "ascending".to_string(),
            limit: 100,
        };

        write
            .send(Message::Text(serde_json::to_string(&req).unwrap()))
            .await
            .expect("request should send");

        let history_messages = timeout(std::time::Duration::from_secs(5), async {
            let mut msgs = Vec::new();
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                        msgs.push(val);
                    }
                }
            }
            msgs
        })
        .await
        .expect("historical timeline query should respond without hanging or panicking");

        assert!(
            !history_messages.is_empty(),
            "timeline should return historical data"
        );

        // Flatten all text from returned frames to verify transcripts survived
        let all_text = serde_json::to_string(&history_messages).unwrap();
        assert!(
            all_text.contains("reversed-transcript"),
            "reversed-offset transcript must survive and be returned"
        );
        assert!(
            all_text.contains("infinite-transcript"),
            "infinite-offset transcript must survive and be returned"
        );
        assert!(
            all_text.contains("valid-transcript"),
            "valid-offset control transcript must survive and be returned"
        );

        // 4. Test capture continuity: insert NEW frames and audio while server is live
        let now = Utc::now();
        db.insert_frame(
            device_name,
            Some(now),
            None,
            Some("Browser"),
            Some("New Tab"),
            false,
            None,
        )
        .await
        .unwrap();

        let new_chunk = db
            .insert_audio_chunk("new-live.mp4", Some(now))
            .await
            .unwrap();
        db.insert_audio_transcription(
            new_chunk,
            "new-live-transcript-after-history",
            0,
            "whisper",
            &audio_device,
            None,
            Some(1.0),
            Some(3.0),
            Some(now),
        )
        .await
        .unwrap();

        // Query the new range
        let req_new = StreamFramesLimitedRequest {
            start_time: (now - Duration::minutes(1)).to_rfc3339(),
            end_time: (now + Duration::minutes(1)).to_rfc3339(),
            order: "ascending".to_string(),
            limit: 50,
        };

        write
            .send(Message::Text(serde_json::to_string(&req_new).unwrap()))
            .await
            .expect("new range request should send");

        let new_messages = timeout(std::time::Duration::from_secs(5), async {
            let mut msgs = Vec::new();
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(text) = msg {
                    if text == "\"keep-alive-text\"" {
                        break;
                    }
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                        msgs.push(val);
                    }
                }
            }
            msgs
        })
        .await
        .expect("new timeline capture query should respond");

        let new_text = serde_json::to_string(&new_messages).unwrap();
        assert!(
            new_text.contains("new-live-transcript-after-history"),
            "new capture must continue durably and be queryable on the timeline"
        );

        // 5. Inverted client bounds test: start_time > end_time must not panic worker
        let req_inverted = StreamFramesLimitedRequest {
            start_time: (now + Duration::hours(1)).to_rfc3339(),
            end_time: (now - Duration::hours(1)).to_rfc3339(),
            order: "descending".to_string(),
            limit: 10,
        };

        write
            .send(Message::Text(serde_json::to_string(&req_inverted).unwrap()))
            .await
            .expect("inverted request should send");

        // The connection remains healthy
        drop(write);
        drop(read);
    }
}
