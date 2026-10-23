use anyhow::anyhow;
use redsuite_core::{
    catalog::{ErGroup, ProxyMode},
    netfault::BaseProxies,
    topology::{self, PrivateEr},
    BaseCtx, ErCtx, Result,
};
use std::rc::Rc;
use tokio::sync::Mutex;

pub(super) struct GroupEr {
    owner: Option<Rc<Mutex<PrivateEr>>>,
    ctx: ErCtx,
    proxies: Option<Rc<BaseProxies>>,
    ws_proxies: Option<Rc<BaseProxies>>,
}

impl GroupEr {
    pub(super) fn ctx(&self) -> &ErCtx {
        &self.ctx
    }

    pub(super) async fn boot(group: &ErGroup, base: &Result<BaseCtx>) -> Result<Self> {
        let base = base.as_ref().map_err(|error| anyhow!("{error:#}"))?;
        if let Some(endpoints) = base.config().environment.as_ref().and_then(|env| env.er.as_ref())
        {
            return Ok(Self {
                owner: None,
                ctx: ErCtx::attach(endpoints).await?,
                proxies: None,
                ws_proxies: None,
            });
        }
        let mut options = group.options();
        let (proxies, ws_proxies) = match group.proxies {
            ProxyMode::None => (None, None),
            ProxyMode::Shared => (Some(Rc::new(BaseProxies::spawn(base).await?)), None),
            ProxyMode::Split => {
                let ws = Rc::new(BaseProxies::spawn(base).await?);
                let http = Rc::new(BaseProxies::spawn(base).await?);
                (Some(http), Some(ws))
            }
        };
        if let Some(proxies) = &proxies {
            let mut endpoints = proxies.endpoints();
            if let Some(ws) = &ws_proxies {
                endpoints.ws_url = ws.endpoints().ws_url;
            }
            options.base_endpoints = Some(endpoints);
        }
        let mut er = if group.followers == 0 {
            topology::private_er(base, options).await?
        } else {
            topology::leader_er(base, options, group.allowed_followers()?).await?
        };
        if let Some(proxies) = &proxies {
            er = er.with_proxies(proxies.clone());
        }
        if let Some(ws) = &ws_proxies {
            er = er.with_ws_proxy(ws.clone());
        }
        er.wait_ready(topology::ER_READY_TIMEOUT).await?;
        let owner = er.shared();
        let ctx = owner.lock().await.ctx().clone();
        Ok(Self {
            owner: Some(owner),
            ctx,
            proxies,
            ws_proxies,
        })
    }

    pub(super) async fn finish(self) -> Result<()> {
        let mut result = Ok(());
        if let Some(er) = self.owner.and_then(Rc::into_inner) {
            if let Err(error) = er.into_inner().finish().await {
                result = Err(error);
            }
        }
        for proxies in [self.proxies, self.ws_proxies].into_iter().flatten() {
            let Some(proxies) = Rc::into_inner(proxies) else { continue };
            if let Err(error) = proxies.finish() {
                result = result.and(Err(error));
            }
        }
        result
    }
}
