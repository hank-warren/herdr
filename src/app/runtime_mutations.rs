use crate::api::schema::{
    LayoutSetSplitRatioParams, Method, PaneFocusDirectionParams, PaneRenameParams,
    PaneResizeParams, PaneSplitParams, PaneSwapParams, PaneTarget, PaneZoomParams, TabTarget,
    WorkspaceTarget,
};

use super::App;

impl App {
    pub(crate) fn dispatch_runtime_mutation(&mut self, id: &'static str, method: Method) -> String {
        self.dispatch_api_request(id, method)
    }

    pub(crate) fn dispatch_deferred_runtime_mutation(
        &mut self,
        id: &'static str,
        method: Method,
    ) -> Option<String> {
        self.dispatch_deferred_api_request(id, method)
    }

    pub(crate) fn runtime_workspace_focus(
        &mut self,
        id: &'static str,
        workspace_id: String,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::WorkspaceFocus(WorkspaceTarget { workspace_id }))
    }

    pub(crate) fn runtime_workspace_close(
        &mut self,
        id: &'static str,
        workspace_id: String,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::WorkspaceClose(WorkspaceTarget { workspace_id }))
    }

    pub(crate) fn runtime_tab_focus(&mut self, id: &'static str, tab_id: String) -> String {
        self.dispatch_runtime_mutation(id, Method::TabFocus(TabTarget { tab_id }))
    }

    pub(crate) fn runtime_tab_close(&mut self, id: &'static str, tab_id: String) -> String {
        self.dispatch_runtime_mutation(id, Method::TabClose(TabTarget { tab_id }))
    }

    pub(crate) fn runtime_pane_focus(&mut self, id: &'static str, pane_id: String) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneFocus(PaneTarget { pane_id }))
    }

    pub(crate) fn runtime_pane_close(&mut self, id: &'static str, pane_id: String) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneClose(PaneTarget { pane_id }))
    }

    pub(crate) fn runtime_pane_rename(
        &mut self,
        id: &'static str,
        params: PaneRenameParams,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneRename(params))
    }

    pub(crate) fn runtime_pane_focus_direction(
        &mut self,
        id: &'static str,
        params: PaneFocusDirectionParams,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneFocusDirection(params))
    }

    pub(crate) fn runtime_pane_resize(
        &mut self,
        id: &'static str,
        params: PaneResizeParams,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneResize(params))
    }

    pub(crate) fn runtime_pane_swap(&mut self, id: &'static str, params: PaneSwapParams) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneSwap(params))
    }

    pub(crate) fn runtime_pane_split(
        &mut self,
        id: &'static str,
        params: PaneSplitParams,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneSplit(params))
    }

    pub(crate) fn runtime_pane_zoom(&mut self, id: &'static str, params: PaneZoomParams) -> String {
        self.dispatch_runtime_mutation(id, Method::PaneZoom(params))
    }

    pub(crate) fn runtime_layout_set_split_ratio(
        &mut self,
        id: &'static str,
        params: LayoutSetSplitRatioParams,
    ) -> String {
        self.dispatch_runtime_mutation(id, Method::LayoutSetSplitRatio(params))
    }
}
