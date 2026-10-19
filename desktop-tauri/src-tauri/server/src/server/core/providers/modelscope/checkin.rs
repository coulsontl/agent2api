//! ModelScope 每日签到：**读一次登录信息就完成**（2026-10-16 实测 + 插件源码对照）。
//!
//! ── 上游是什么 ──────────────────────────────────────────────
//! 魔搭**没有独立的签到接口**（官方网页的「每日签到」按钮背后就是这条）：
//!
//! ```text
//! GET {网页站}/api/v1/users/login/info          Cookie: m_session_id=<ms-令牌>
//!   → {"Code":200,"Success":true,"Data":{"Name":"…","NickName":"…",
//!                                          "HavanaId":"…","Email":"…"}, …}
//! ```
//!
//! **调用这个端点本身就触发当日的魔粒签到**（激活当日魔粒额度），
//! 返回体里给的是账号身份，而不是「签到成功」这四个字 ——
//! 这是本家签到与别家最大的不同，也是「看起来只是读了个用户信息」的原因。
//!
//! ── 依据来自哪里（不是猜的）─────────────────────────────────
//! CPA 的 `modelscope-ratelimit` 插件（v2.4.10，`cmd/plugin/main.go` 的午夜签到段）
//! 就是按这个口径做的：遍历账号 → 调 `/api/v1/users/login/info` →
//! 把这次调用登记成当日签到（`store.RecordCheckIn(aid, now)`）。
//! 本实现照同一条链路，只是把触发时机交给本项目的自动签到框架。
//!
//! ── 幂等 ────────────────────────────────────────────────────
//! 一天内调多次是安全的（插件同样每天只调一次，但没有「今天已签过」的拒绝返回）。
//! 因此「今天已经触发过」**不算失败** —— 如实报成功并说明是重复调用，
//! 与 Loomy 的 `first-login` 同一口径。
//!
//! ── 两个地区打各自的站 ──────────────────────────────────────
//! `region` 决定站点。打错了会拿到 `401 access forbidden`（实测：国内站的令牌打
//! 国际站就是这个响应），这里把它如实翻成「令牌不属于该站点」而不是笼统的失败 ——
//! 那正是用户把两地账号加混时最需要看到的一句话。
//!
//! ── 硬约束 ──────────────────────────────────────────────────
//! release 是 `panic=abort`：本文件零 unwrap/expect/panic。

use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::errors::GatewayError;
use crate::server::logging;

use super::client;
use super::credentials;
use super::region::Region;
use crate::server::core::proxies::ResolvedProxy;

/// 登录信息路径（相对网页站）。**它就是签到**，见模块头。
pub const LOGIN_INFO_PATH: &str = "/api/v1/users/login/info";

/// 魔粒数的展示（整数不显示小数点，小数保留原样）。
///
/// 上游的魔粒是整数，但**别假设它永远是**：按 `f64` 收、按有无小数位格式化，
/// 上游哪天改成小数不会让提示变成「已领取 0.30000000000000004 魔粒」。
fn format_amount(value: Option<f64>) -> String {
    match value {
        Some(number) if number.fract() == 0.0 => format!("{}", number as i64),
        Some(number) => format!("{number}"),
        None => "—".to_string(),
    }
}

/// 读一次魔粒余额（失败给 None）。
///
/// 签到前后各读一次算增量，所以这条链路上**不该**因为读不到余额就把签到判失败
/// —— 少一个读数不等于签到没成功（与 OfficeAce 的同一处置）。
async fn read_balance(region: Region, token: &str, proxy: Option<&ResolvedProxy>) -> Option<f64> {
    let payload = client::site_get(
        region,
        super::balance::BALANCE_PATH,
        token,
        proxy,
        "魔粒余额",
    )
    .await
    .ok()?;
    if !client::succeeded(&payload) {
        return None;
    }
    let data = client::data_of(&payload);
    data.get("available_balance").and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.trim().parse::<f64>().ok()))
    })
}

/// ModelScope 账号的每日签到（自动签到框架的 claim）。
///
/// 返回 `{success, msg}` —— 与 WorkBuddy / 小浣熊 / AutoClaw / Qoder / Loomy
/// 的 claim 同一形状（`billing::checkin` 的汇总只认这两个字段）。
///
/// ── 为什么播报**领取增量**而不是「已触发」────────────────────────
/// `login/info` 是幂等读接口（见模块头）：今天第一次调用发魔粒、之后每次都只回
/// 同一份资料。所以「已触发」这句话在有 5 个账号的签到中心里等于没说 ——
/// 用户看不出今天到底领到没有、领了多少。签到前后各读一次余额、把**差值**
/// 播报出来，一眼就能分辨「刚领到 200」与「今天早就签过了」；再把当前余额带上，
/// 不必再去账号页看一眼。读不到余额时逐字退回「已签到」，不编一个数字。
pub async fn claim_daily_checkin(
    region: Region,
    store: &AccountStore,
    account_id: &str,
) -> Result<Value, GatewayError> {
    let record = store.modelscope_account_record(region, account_id);
    if !account_id.is_empty() && record.is_none() {
        return Err(GatewayError::with_status(
            404,
            format!(
                "ModelScope {}账号 {account_id} 不存在（请重新添加）",
                region.label()
            ),
        ));
    }
    let credentials = credentials::from_record(region, record.as_ref())?;
    // 账号级出网代理（用户给这条账号配的出口要一路带上，见 `client::account_proxy`）
    let proxy = client::account_proxy(record.as_ref())?;

    // 签到前记一份余额（算增量用）。读失败不阻断签到 —— 见 `read_balance`。
    let before = read_balance(region, &credentials.token, proxy.as_ref()).await;

    let payload = match client::site_get(
        region,
        LOGIN_INFO_PATH,
        &credentials.token,
        proxy.as_ref(),
        "每日签到",
    )
    .await
    {
        Ok(payload) => payload,
        // 401 原样透出（调用方按「刷新后重试」的既有处置走；本家没有续期，
        // 最终会如实报「请重新粘贴令牌」）
        Err(error) => return Err(error),
    };

    if !client::succeeded(&payload) {
        let message = client::message_of(&payload);
        let message = if message.trim().is_empty() {
            format!("每日签到失败（上游 Code={}）", client::code_of(&payload))
        } else {
            format!("每日签到失败：{message}")
        };
        logging::log("[Checkin]", &format!("⚠️ ModelScope {message}"));
        return Ok(json!({ "success": false, "msg": message }));
    }

    // 成功时上游给的是账号身份（不是「签到成功」）—— 把显示名播报出来，
    // 用户一眼能确认「这条记录签的是哪个账号」（多账号场景下这是有用的信息）
    let data = client::data_of(&payload);
    let display = data
        .get("NickName")
        .or_else(|| data.get("Name"))
        .or_else(|| data.get("nickname"))
        .or_else(|| data.get("name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .to_string();

    // 签到后余额（同样容错：读不到就退回「已签到」，不编数字）
    let after = read_balance(region, &credentials.token, proxy.as_ref()).await;
    let gained = match (before, after) {
        (Some(before), Some(after)) => Some(after - before),
        _ => None,
    };

    let msg = match gained {
        Some(delta) if delta > 0.0 => format!(
            "已领取 {} 魔粒（当前 {}）",
            format_amount(Some(delta)),
            format_amount(after)
        ),
        Some(_) if after.is_some() => format!(
            "今天已经签到过了（当前 {}）",
            format_amount(after)
        ),
        _ => "已签到".to_string(),
    };
    // 显示名单独一句：增量是主读数，账号身份是补充（多账号时用来对账）
    let msg = if display.is_empty() {
        msg
    } else {
        format!("{msg}｜{display}")
    };
    logging::log("[Checkin]", &format!("✅ 魔搭{}：{msg}", region.label()));
    Ok(json!({ "success": true, "msg": msg, "gained": gained, "balance": after }))
}