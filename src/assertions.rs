//! 统一断言框架：pass / fail / skip 报告
//!
//! 每条断言有名称、条件、实际值、阈值，执行后输出统一格式。

use std::fmt;

/// 断言结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssertionStatus {
    Pass,
    Fail,
    Skip,
}

impl fmt::Display for AssertionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pass => write!(f, "PASS"),
            Self::Fail => write!(f, "FAIL"),
            Self::Skip => write!(f, "SKIP"),
        }
    }
}

/// 单条断言结果
pub struct AssertionResult {
    pub name: String,
    pub status: AssertionStatus,
    pub actual: String,
    pub threshold: String,
}

/// 断言报告器
pub struct AssertionReport {
    results: Vec<AssertionResult>,
}

impl AssertionReport {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    /// 断言：actual >= threshold
    pub fn assert_gte(&mut self, name: &str, actual: f64, threshold: f64) {
        let status = if actual >= threshold {
            AssertionStatus::Pass
        } else {
            AssertionStatus::Fail
        };
        self.results.push(AssertionResult {
            name: name.to_string(),
            status,
            actual: format!("{:.2}", actual),
            threshold: format!(">= {:.2}", threshold),
        });
    }

    /// 断言：actual <= threshold
    pub fn assert_lte(&mut self, name: &str, actual: f64, threshold: f64) {
        let status = if actual <= threshold {
            AssertionStatus::Pass
        } else {
            AssertionStatus::Fail
        };
        self.results.push(AssertionResult {
            name: name.to_string(),
            status,
            actual: format!("{:.2}", actual),
            threshold: format!("<= {:.2}", threshold),
        });
    }

    /// 跳过（条件不满足时不执行断言）
    pub fn skip(&mut self, name: &str, reason: &str) {
        self.results.push(AssertionResult {
            name: name.to_string(),
            status: AssertionStatus::Skip,
            actual: reason.to_string(),
            threshold: "-".to_string(),
        });
    }

    /// 打印报告
    pub fn print(&self) {
        println!("\n========== 断言报告 ==========");
        let mut pass = 0u32;
        let mut fail = 0u32;
        let mut skip = 0u32;

        for r in &self.results {
            let icon = match r.status {
                AssertionStatus::Pass => "✅",
                AssertionStatus::Fail => "❌",
                AssertionStatus::Skip => "⏭️",
            };
            println!(
                "  {} [{}] {} | actual={} threshold={}",
                icon, r.status, r.name, r.actual, r.threshold
            );
            match r.status {
                AssertionStatus::Pass => pass += 1,
                AssertionStatus::Fail => fail += 1,
                AssertionStatus::Skip => skip += 1,
            }
        }

        println!("------------------------------");
        println!("  合计: {} pass, {} fail, {} skip", pass, fail, skip);
        println!("==============================\n");
    }

    /// 是否全部通过（无 FAIL）
    pub fn all_passed(&self) -> bool {
        !self
            .results
            .iter()
            .any(|r| r.status == AssertionStatus::Fail)
    }

    /// 失败数
    pub fn fail_count(&self) -> u32 {
        self.results
            .iter()
            .filter(|r| r.status == AssertionStatus::Fail)
            .count() as u32
    }
}
