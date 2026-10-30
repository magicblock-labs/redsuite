use anyhow::anyhow;
use redsuite_core::{
    catalog::ErGroup,
    netfault::Proxy,
    topology::{self, BaseEndpoints, PrivateEr},
    BaseCtx, ErCtx, Result,
};
use std::rc::Rc;
use tokio::sync::Mutex;

pub(super) struct GroupEr {
    owner: Option<Rc<Mutex<PrivateEr>>>,
    ctx: ErCtx,
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
            });
        }
        let mut options = group.options();
        let proxy = if group.proxy {
            Some(Rc::new(
                Proxy::spawn(BaseEndpoints::from_base(base)?).await?,
            ))
        } else {
            None
        };
        if let Some(proxy) = &proxy {
            options.base_endpoints = Some(proxy.endpoints());
        }
        let mut er = if group.followers == 0 {
            topology::private_er(base, options).await?
        } else {
            topology::leader_er(base, options, group.allowed_followers()?).await?
        };
        if let Some(proxy) = proxy {
            er = er.with_proxy(proxy);
        }
        er.wait_ready(topology::ER_READY_TIMEOUT).await?;
        let owner = er.shared();
        let ctx = owner.lock().await.ctx().clone();
        Ok(Self { owner: Some(owner), ctx })
    }

    pub(super) async fn finish(self) -> Result<()> {
        if let Some(er) = self.owner.and_then(Rc::into_inner) {
            er.into_inner().finish().await?;
        }
        Ok(())
    }
}
