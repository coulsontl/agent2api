//! 把 models.dev 的元数据补进一条清单项（**清单成型处唯一的写入点**）。
//!
//! ── 优先级（本模块只有这一条规则）───────────────────────────
//! **用户覆盖 > 上游声明 > models.dev > 未声明**。因此这里只做一件事：
//! 清单项里**没有**某键（缺失或 null）时，用 models.dev 的值补上；上游已经
//! 声明过的键**一律不动**。用户覆盖层在调用方之后执行（`model_rules` 的
//! `apply_capability_overrides`），它天然排在最后。
//!
//! 为什么上游优先而不是 models.dev 优先：上游那份是**这条转发通道**的真实
//! 情况（小浣熊的服务端就下发 `supportThinking`、CatPaw 下发
//! `support_thinking`），而 models.dev 记的是模型在**实验室自己那套 API** 上的
//! 能力 —— 同一模型经不同中转通道能收的东西并不相同。用后者推翻前者，等于拿
//! 一份外部数据否认本通道的实测事实。
//!
//! ── 来源标记（`capSources`）─────────────────────────────────
//! 每个被填上的键都记一句「这个值是哪来的」（`upstream` = 清单里本来就有、
//! `modelsdev` = 本次补的、`user` = 用户覆盖层改的）。管理页据此把「真实值」
//! 与「我们猜的 / 数据库说的」分开显示 —— 没有这个标记，界面上的六个键看起来
//! 一样可信，而它们的可信度差着量级。
//!
//! ── 思考档位的归一（同一处完成）─────────────────────────────
//! 三家各用自己的键名声明「这个模型认识哪些档位」：ZCode 是
//! `reasoningLevels`（本就是出口键名），Accio 是 `reasoningEfforts`，Qoder 是
//! `efforts`。这些键**各自有主**（三家的适配器按它们翻译/回退，见
//! `providers/accio/protocol.rs`、`providers/qoder/protocol.rs`），本模块只
//! **只读**地把它们归一成出口那一对键（`reasoningLevels` +
//! `reasoningDefaultLevel`），原键一个都不改、不删。
//!
//! 归一后的档位列表来源优先级：**这家自己声明 > models.dev 的 effort 列表**。
//! 都拿不到就**不出这个键** —— 前端据此回落通用档位表（「未声明」与「一档都
//! 没有」对用户是两件事，见 `ui-islands` 的 `model-capability.ts`）。

use serde_json::{json, Map, Value};

use super::snapshot::{self, Record};

/// 六键与 models.dev 字段的对应关系（顺序与 `capability::KEYS` 一致）。
///
/// 这份对应只写一次：出口的键名由 `capability` 决定，models.dev 的字段名由
/// 同步脚本决定，两者在这里汇合。
const BOOL_BRIDGE: [(&str, fn(&Record) -> Option<bool>); 4] = [
    ("supportsToolCall", |record| record.tool_call),
    ("supportsImages", |record| record.images),
    ("supportsVideo", |record| record.video),
    ("supportsReasoning", |record| record.reasoning),
];

const TOKEN_BRIDGE: [(&str, fn(&Record) -> Option<u64>); 2] = [
    ("maxInputTokens", |record| record.context),
    ("maxOutputTokens", |record| record.output),
];

/// 各家声明「本模型认识哪些档位」用的键名（按优先级：出口键名在前）。
///
/// 前一个是出口键本身（ZCode 直接用它），后两个是两家的内部键名 —— 归一了
/// 出口就统一了，但**不删原键**：那两家转发时仍按自己的键读（理由见模块头）。
const LEVEL_KEYS: [&str; 3] = ["reasoningLevels", "reasoningEfforts", "efforts"];

/// 只应用「模型本体」那一档实测事实（不动 models.dev 的补齐）。
///
/// 给**没过 [`apply`] 的读路径**用：自定义家的闸门读数是
/// `custom_providers::models_of` + 展平（见 `catalog::routing::manifest_entry_for`），
/// 那条路径不经过本模块，于是这份事实要单独补一次。
///
/// 幂等：条目已经声明过该键时原样不动。
pub fn apply_intrinsic_only(item: &mut Value) {
    let Some(id) = item.get("id").and_then(Value::as_str).map(str::to_string) else { return };
    let Some(object) = item.as_object_mut() else { return };
    let mut sources = object
        .get("capSources")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    apply_intrinsic_facts(object, &id, &mut sources);
    if !sources.is_empty() {
        object.insert("capSources".to_string(), Value::Object(sources));
    }
}

/// 把「模型本体」的实测事实写进条目，并把来源记成 `observed`。
///
/// **刻意排在 `snapshot::lookup` 之外**：这份事实是我们自己实测的，社区库有没有
/// 收录这条模型与它无关。2026-10-17 的草稿把它关在 `if let Some(record)` 里面，
/// 而 `glm-5.3` 在 models.dev 里查不到 ⇒ 整段被跳过 ⇒ 标记等于没写。
///
/// 上游已声明的键不动（那是这条通道的真实情况，优先级更高），只记来源。
pub(crate) fn apply_intrinsic_facts(object: &mut Map<String, Value>, id: &str, sources: &mut Map<String, Value>) {
    for (key, _) in BOOL_BRIDGE {
        if object.get(key).is_some_and(|value| !value.is_null()) {
            sources.insert(key.to_string(), json!("upstream"));
            continue;
        }
        if let Some(flag) = model_intrinsic_flag(id, key) {
            object.insert(key.to_string(), Value::Bool(flag));
            sources.insert(key.to_string(), json!("observed"));
        }
    }
}

/// **模型本体**（不是某条通道）的实测能力位。
///
/// 与上游声明、models.dev 都不同的第三档事实：
///   - **上游声明** = 「这条转发通道收不收」—— 各家可能不同（models.dev 记的是
///     实验室口径，所以 `entry_refuses_images` 不认它）；
///   - **本函数** = 「这个模型本身有没有这个能力」—— 与走哪个渠道无关，任何家
///     拿到同一个 id 都是同一个答案。
///
/// 位置为什么在这里：`apply` 是「一家全部出口的唯一源头」，内置家与自定义家都
/// 走它，所以在这一层写一次，14 家全部生效。放到某一家的 `map_entry` 里只能
/// 修那一家（officeace 那份草稿就是反例）。
///
/// 判据是**精确 id**，不做前缀：`glm-5.3-flash` 能吃图，拿 `glm-5.3` 当前缀
/// 会把它一起拦掉。每一条都必须有出处，不许凭名字猜（`glm-5` 与 `glm-5.3`
/// 的能力就不一样）。
fn model_intrinsic_flag(id: &str, key: &str) -> Option<bool> {
    match (id, key) {
        // 2026-10-17 面板实测（OfficeAce 果办）+ 2026-09-30 实测（ZCode）两处互证：
        // 同一个模型、同一句上游原文，说明这是模型本体的口径而不是某家的：
        //   `400 messages.content.type 参数非法，取值范围 ['text']`
        // ZCode 那条记录在 `providers/zcode/models.rs` 的 `MODELS` 表上方。
        ("glm-5.3", "supportsImages") => Some(false),
        _ => None,
    }
}

/// 把 models.dev 的值补进这条清单项（原地；上游已声明的键不动）。
///
/// 调用方只有两处，都是「一家全部出口的唯一源头」：内置家的
/// `providers::catalog::manifest_for`、自定义家的
/// `custom_providers::bindings::public_models`。收在两处就不可能出现「管理页
/// 改了、下游没变」那种分叉（与能力位覆盖层同一理由）。
pub fn apply(item: &mut Value) {
    let Some(id) = item.get("id").and_then(Value::as_str).map(str::to_string) else { return };
    let record = snapshot::lookup(&id);
    let mut sources: Map<String, Value> = Map::new();
    let Some(object) = item.as_object_mut() else { return };
    apply_intrinsic_facts(object, &id, &mut sources);
    if let Some(record) = &record {
        for (key, pick) in BOOL_BRIDGE {
            match object.get(key) {
                // 上游声明过 / 实测表已填：岿然不动（来源已在上面记好）
                Some(value) if !value.is_null() => {}
                _ => {
                    if let Some(flag) = pick(record) {
                        object.insert(key.to_string(), Value::Bool(flag));
                        sources.insert(key.to_string(), json!("modelsdev"));
                    }
                }
            }
        }
        for (key, pick) in TOKEN_BRIDGE {
            match object.get(key) {
                Some(value) if !value.is_null() => {
                    sources.insert(key.to_string(), json!("upstream"));
                }
                _ => {
                    if let Some(number) = pick(record) {
                        object.insert(key.to_string(), Value::from(number));
                        sources.insert(key.to_string(), json!("modelsdev"));
                    }
                }
            }
        }
    } else {
        // 没收录也要把上游声明记下来：管理页据此说明「这个值来自上游」，
        // 而不是显示成「没数据」。
        for key in BOOL_BRIDGE.iter().map(|(key, _)| *key).chain(TOKEN_BRIDGE.iter().map(|(key, _)| *key)) {
            if object.get(key).is_some_and(|value| !value.is_null()) {
                sources.insert(key.to_string(), json!("upstream"));
            }
        }
    }
    if !sources.is_empty() {
        if let Some(object) = item.as_object_mut() {
            object.insert("capSources".to_string(), Value::Object(sources));
        }
    }
    normalize_levels(item, record.as_ref());
}

/// 归一逐模型档位（原键只读，见模块头）。
fn normalize_levels(item: &mut Value, record: Option<&Record>) {
    let declared = LEVEL_KEYS
        .iter()
        .find_map(|key| item.get(*key).and_then(levels_of))
        .filter(|levels| !levels.is_empty());
    let levels = match declared {
        Some(levels) => levels,
        None => record.map(|record| record.levels.clone()).unwrap_or_default(),
    };
    if levels.is_empty() {
        return;
    }
    if let Some(object) = item.as_object_mut() {
        object.insert(
            "reasoningLevels".to_string(),
            Value::Array(levels.into_iter().map(Value::String).collect()),
        );
    }
}

/// 从清单项的一个档位键里读出档位表（非数组 / 空串 / 数字一律丢弃）。
fn levels_of(value: &Value) -> Option<Vec<String>> {
    let items = value.as_array()?;
    Some(
        items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|level| !level.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// 用户覆盖层改过的键，来源改记成 `user`（在覆盖层里调用）。
///
/// 覆盖值是**用户明确指定**的，既不是上游也不是 models.dev —— 管理页要把这条
/// 区分出来，否则用户会以为那是清单里的原值。
///
/// `sources` 的三个取值（`upstream` / `modelsdev` / `user`）是**前后端契约**：
/// 前端按同一批字面量渲染标记（`ui-islands` 的 `model-capability.ts`）。
pub fn mark_user(item: &mut Value, keys: &[String]) {
    if keys.is_empty() {
        return;
    }
    let Some(object) = item.as_object_mut() else { return };
    let mut sources = object
        .get("capSources")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for key in keys {
        sources.insert(key.clone(), json!("user"));
    }
    object.insert("capSources".to_string(), Value::Object(sources));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{apply, model_intrinsic_flag};

    /// 「模型本体」那一档事实：精确 id、只对已知的键生效。
    ///
    /// 钉住两件事：命中的模型被标成不吃图；**同族的变体不许被前缀误伤**
    /// （`glm-5.3-flash` 能吃图，ZCode 侧 2026-09-30 发红色 PNG 实测过）。
    #[test]
    fn the_intrinsic_table_is_exact_and_narrow() {
        assert_eq!(Some(false), model_intrinsic_flag("glm-5.3", "supportsImages"));
        assert_eq!(
            None,
            model_intrinsic_flag("glm-5.3-flash", "supportsImages"),
            "flash 变体能吃图，不许被前缀误伤"
        );
        assert_eq!(None, model_intrinsic_flag("glm-5.3", "supportsVideo"), "只标实测过的那个键");
        assert_eq!(None, model_intrinsic_flag("glm-5", "supportsImages"), "同族不同代，能力不一样");
        assert_eq!(None, model_intrinsic_flag("GLM-5.3", "supportsImages"), "判据是精确 id，大小写不归一");
    }

    /// 上游没声明时，`observed` 排在 models.dev 前面并留下自己的来源标记。
    ///
    /// 这一条是整个改动的落点：`glm-5.3` 在 OfficeAce 的目录里本来什么能力位都没有，
    /// 靠这里补上 `supportsImages: false` + `capSources: observed`，闸门才认得出来。
    #[test]
    fn an_unclaimed_key_gets_the_observed_fact_before_modelsdev() {
        let mut item = json!({"id": "glm-5.3"});
        apply(&mut item);
        assert_eq!(json!(false), item["supportsImages"], "实测表要补上 false");
        assert_eq!(
            json!("observed"),
            item["capSources"]["supportsImages"],
            "来源要标 observed：它不是社区库猜的，闸门按它判"
        );
    }

    /// 实测表**不依赖 models.dev 收录**：社区库查不到这个模型时它照样要生效。
    ///
    /// 这条是 2026-10-17 端到端验证时抓到的真 bug：`model_intrinsic_flag` 原先
    /// 写在 `if let Some(record)` 里面，而 `glm-5.3` 在 models.dev 里查不到 ⇒
    /// 整个分支被跳过 ⇒ 目录里仍然什么都不标 ⇒ 闸门照样放行。实测事实凭什么
    /// 要被一份外部数据的存在与否卡住？现在它排在那层判断之外。
    #[test]
    fn the_observed_fact_does_not_depend_on_modelsdev_coverage() {
        // 直接测「写事实」那一层：它不接收 record，从签名上就保证了与收录无关。
        // 这正是 `glm-5.3` 的实际处境（models.dev 里没有这条）。
        let mut item = json!({"id": "glm-5.3"});
        let mut sources = serde_json::Map::new();
        let object = item.as_object_mut().expect("对象");
        super::apply_intrinsic_facts(object, "glm-5.3", &mut sources);
        assert_eq!(json!(false), item["supportsImages"], "models.dev 没收录也挡不住实测事实");
        assert_eq!(json!("observed"), sources["supportsImages"]);
    }

    /// 上游**自己声明过**的键，实测表不许改 —— 那是这条通道的真实情况。
    #[test]
    fn an_upstream_declaration_is_never_overridden() {
        let mut item = json!({"id": "glm-5.3", "supportsImages": true});
        apply(&mut item);
        assert_eq!(json!(true), item["supportsImages"], "上游说能就是能，实测表不覆盖");
        assert_eq!(json!("upstream"), item["capSources"]["supportsImages"]);
    }
}
