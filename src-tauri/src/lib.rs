pub mod p2p;

use p2p::{ChatMessagePayload, ChatNode, DirectoryUser, GroupRoom, PeerInfo, StateSnapshot};
use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager, State, WindowEvent};
use tokio::sync::OnceCell;

pub struct AppState {
    pub node: OnceCell<Arc<ChatNode>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            node: OnceCell::new(),
        }
    }

    pub async fn get_node(&self, app_handle: &AppHandle) -> Result<Arc<ChatNode>, String> {
        let handle = app_handle.clone();
        self.node
            .get_or_try_init(|| async move {
                let profile = std::env::var("SIMPLE_CHAT_PROFILE")
                    .ok()
                    .filter(|s| !s.trim().is_empty());
                let file_name = match profile {
                    Some(p) => format!("config-{}.json", p.trim()),
                    None => "config.json".to_string(),
                };
                let config_path = handle
                    .path()
                    .app_config_dir()
                    .ok()
                    .map(|dir| dir.join(file_name));

                let cb_handle = handle.clone();
                ChatNode::new_with_config(
                    Arc::new(move |event| {
                        let _ = cb_handle.emit("p2p-event", event);
                    }),
                    config_path,
                )
                .await
            })
            .await
            .map(Arc::clone)
            .map_err(|e| e.to_string())
    }
}

#[tauri::command]
async fn get_state_snapshot(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.snapshot().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn set_nickname(
    nickname: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    *node.nickname.write().await = nickname.trim().to_string();
    node.save_config().await;
    let snap = node.snapshot().await.map_err(|e| e.to_string())?;
    let _ = app.emit(
        "p2p-event",
        p2p::P2PEvent::StateUpdated {
            snapshot: snap.clone(),
        },
    );
    Ok(snap)
}

#[tauri::command]
async fn register_user(
    server_url: String,
    username: String,
    nickname: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.register_on_server(server_url, username, nickname)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn fetch_online_users(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<Vec<DirectoryUser>, String> {
    let node = state.get_node(&app).await?;
    node.fetch_online_users().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn connect_user(
    username: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<PeerInfo, String> {
    let node = state.get_node(&app).await?;
    node.connect_by_username(&username)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn connect_peer(
    ticket: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<PeerInfo, String> {
    let node = state.get_node(&app).await?;
    node.connect_by_ticket_verified(&ticket, None)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn send_message(
    content: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<ChatMessagePayload, String> {
    let node = state.get_node(&app).await?;
    node.send_chat_message(content)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn create_group_room(
    room_name: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.create_group_room(room_name)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn fetch_group_rooms(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<Vec<GroupRoom>, String> {
    let node = state.get_node(&app).await?;
    node.fetch_group_rooms().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn join_group_room(
    room_id: String,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.join_group_room(&room_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn respond_connection_request(
    node_id: Option<String>,
    accept: bool,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.respond_connection_request(node_id, accept)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn clear_chat_history(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<StateSnapshot, String> {
    let node = state.get_node(&app).await?;
    node.clear_messages().await.map_err(|e| e.to_string())
}

#[tauri::command]
async fn export_chat_history(
    formatted_text: String,
    app: AppHandle,
) -> Result<String, String> {
    let dir = app
        .path()
        .download_dir()
        .or_else(|_| app.path().app_config_dir())
        .map_err(|e| e.to_string())?;
    let _ = std::fs::create_dir_all(&dir);
    let ts = p2p::now_millis() / 1000;
    let file_path = dir.join(format!("simple-chat-history-{}.txt", ts));
    std::fs::write(&file_path, formatted_text).map_err(|e| e.to_string())?;
    Ok(file_path.to_string_lossy().to_string())
}

#[tauri::command]
async fn disconnect_peer(
    node_id: Option<String>,
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let node = state.get_node(&app).await?;
    node.disconnect_peer(node_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn toggle_main_window(app: AppHandle) -> Result<(), String> {
    if let Some(main_win) = app.get_webview_window("main") {
        let is_visible = main_win.is_visible().unwrap_or(false);
        if is_visible {
            let _ = main_win.hide();
        } else {
            let _ = main_win.show();
            let _ = main_win.set_focus();
        }
    }
    Ok(())
}

#[tauri::command]
async fn quit_app(app: AppHandle) -> Result<(), String> {
    app.exit(0);
    Ok(())
}

/// Disable WebKitGTK 2.52/2.54 partial damage compositing features on transparent windows
/// (`UseDamagingInformationForCompositing`, `PropagateDamagingInformation`, `UnifyDamagedRegions`),
/// which otherwise cause SHM swapchain buffers to only render the single damaged sub-rect
/// and erase the rest of the window.
#[cfg(target_os = "linux")]
unsafe fn disable_webkit_partial_damage_bugs(webview_ptr: *mut std::ffi::c_void) {
    use std::ffi::{c_char, c_int, c_void, CStr, CString};

    extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    }
    const RTLD_DEFAULT: *mut c_void = std::ptr::null_mut();

    unsafe fn sym(name: &str) -> *mut c_void {
        let c = CString::new(name).unwrap();
        dlsym(RTLD_DEFAULT, c.as_ptr())
    }

    let get_settings_ptr = sym("webkit_web_view_get_settings");
    let get_all_features_ptr = sym("webkit_settings_get_all_features");
    let list_len_ptr = sym("webkit_feature_list_get_length");
    let list_get_ptr = sym("webkit_feature_list_get");
    let feat_id_ptr = sym("webkit_feature_get_identifier");
    let set_feat_ptr = sym("webkit_settings_set_feature_enabled");
    let reload_ptr = sym("webkit_web_view_reload");

    if get_settings_ptr.is_null()
        || get_all_features_ptr.is_null()
        || list_len_ptr.is_null()
        || list_get_ptr.is_null()
        || feat_id_ptr.is_null()
        || set_feat_ptr.is_null()
    {
        return;
    }

    let get_settings: unsafe extern "C" fn(*mut c_void) -> *mut c_void =
        std::mem::transmute(get_settings_ptr);
    let get_all_features: unsafe extern "C" fn() -> *mut c_void =
        std::mem::transmute(get_all_features_ptr);
    let list_len: unsafe extern "C" fn(*mut c_void) -> usize = std::mem::transmute(list_len_ptr);
    let list_get: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void =
        std::mem::transmute(list_get_ptr);
    let feat_id: unsafe extern "C" fn(*mut c_void) -> *const c_char =
        std::mem::transmute(feat_id_ptr);
    let set_feat: unsafe extern "C" fn(*mut c_void, *mut c_void, c_int) =
        std::mem::transmute(set_feat_ptr);

    let settings = get_settings(webview_ptr);
    if settings.is_null() {
        return;
    }

    let feature_list = get_all_features();
    if feature_list.is_null() {
        return;
    }

    let len = list_len(feature_list);
    for i in 0..len {
        let feat = list_get(feature_list, i);
        if feat.is_null() {
            continue;
        }
        let id_cstr = feat_id(feat);
        if id_cstr.is_null() {
            continue;
        }
        if let Ok(id_str) = CStr::from_ptr(id_cstr).to_str() {
            if matches!(
                id_str,
                "UseDamagingInformationForCompositing"
                    | "PropagateDamagingInformation"
                    | "UnifyDamagedRegions"
            ) {
                set_feat(settings, feat, 0);
            }
        }
    }

    if !reload_ptr.is_null() {
        let reload: unsafe extern "C" fn(*mut c_void) = std::mem::transmute(reload_ptr);
        reload(webview_ptr);
    }
}

#[cfg(target_os = "linux")]
fn configure_linux_window_tree(widget: &gtk::Widget, is_overlay: bool) {
    use gtk::glib::translate::ToGlibPtr;
    use gtk::prelude::*;

    if is_overlay {
        widget.set_app_paintable(true);
        widget.connect_draw(|_, cr| {
            cr.set_source_rgba(0.0, 0.0, 0.0, 0.0);
            cr.set_operator(gtk::cairo::Operator::Clear);
            let _ = cr.paint();
            cr.set_operator(gtk::cairo::Operator::Over);
            gtk::glib::Propagation::Proceed
        });
    }

    let type_name = widget.type_().name();
    if type_name.contains("WebView") {
        let ptr: *mut gtk::ffi::GtkWidget = widget.to_glib_none().0;
        unsafe {
            disable_webkit_partial_damage_bugs(ptr as *mut std::ffi::c_void);
        }
    }

    if let Ok(container) = widget.clone().downcast::<gtk::Container>() {
        for child in container.children() {
            configure_linux_window_tree(&child, is_overlay);
        }
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::new())
        .setup(|app| {
            #[cfg(target_os = "linux")]
            {
                use gtk::prelude::*;
                if let Some(overlay_win) = app.get_webview_window("overlay") {
                    if let Ok(gtk_win) = overlay_win.gtk_window() {
                        configure_linux_window_tree(gtk_win.upcast_ref::<gtk::Widget>(), true);
                    }
                }
                if let Some(main_win) = app.get_webview_window("main") {
                    if let Ok(gtk_win) = main_win.gtk_window() {
                        configure_linux_window_tree(gtk_win.upcast_ref::<gtk::Widget>(), false);
                    }
                }
            }

            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let state = handle.state::<AppState>();
                let _ = state.get_node(&handle).await;
            });
            Ok(())
        })
        .on_window_event(|window, event| {
            // Clicking 'X' on the "main" (history) window hides it instead of quitting,
            // so the DesktopMate-like overlay window continues running on the desktop.
            if window.label() == "main" {
                if let WindowEvent::CloseRequested { api, .. } = event {
                    api.prevent_close();
                    let _ = window.hide();
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            get_state_snapshot,
            set_nickname,
            register_user,
            fetch_online_users,
            connect_user,
            connect_peer,
            send_message,
            create_group_room,
            fetch_group_rooms,
            join_group_room,
            respond_connection_request,
            clear_chat_history,
            export_chat_history,
            disconnect_peer,
            toggle_main_window,
            quit_app
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
