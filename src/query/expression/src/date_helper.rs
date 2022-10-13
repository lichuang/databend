// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::Date;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Tz;
use num_traits::AsPrimitive;

use crate::types::date::check_date;
use crate::types::timestamp::check_timestamp;

pub trait DateConverter {
    fn to_date(&self, tz: &Tz) -> Date<Tz>;
    fn to_timestamp(&self, tz: &Tz) -> DateTime<Tz>;
}

impl<T> DateConverter for T
where T: AsPrimitive<i64>
{
    fn to_date(&self, tz: &Tz) -> Date<Tz> {
        let mut dt = tz.ymd(1970, 1, 1);
        dt = dt.checked_add_signed(Duration::days(self.as_())).unwrap();
        dt
    }

    fn to_timestamp(&self, tz: &Tz) -> DateTime<Tz> {
        // Can't use `tz.timestamp_nanos(self.as_() * 1000)` directly, is may cause multiply with overflow.
        let micros = self.as_();
        let (mut secs, mut nanos) = (micros / 1_000_000, (micros % 1_000_000) * 1_000);
        if nanos < 0 {
            secs -= 1;
            nanos += 1_000_000_000;
        }
        tz.timestamp_opt(secs, nanos as u32).unwrap()
    }
}

// Timestamp arithmetic factors.
pub const FACTOR_HOUR: i64 = 3600;
pub const FACTOR_MINUTE: i64 = 60;
pub const FACTOR_SECOND: i64 = 1;

fn add_years_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate, String> {
    let new_year = year + delta as i32;
    let mut new_day = day;
    if std::intrinsics::unlikely(month == 2 && day == 29) {
        new_day = last_day_of_year_month(new_year, month);
    }
    NaiveDate::from_ymd_opt(new_year, month, new_day).ok_or(format!(
        "Overflow on date YMD {}-{}-{}.",
        new_year, month, new_day
    ))
}

fn add_months_base(year: i32, month: u32, day: u32, delta: i64) -> Result<NaiveDate, String> {
    let total_months = month as i64 + delta - 1;
    let mut new_year = year + (total_months / 12) as i32;
    let mut new_month0 = total_months % 12;
    if new_month0 < 0 {
        new_year -= 1;
        new_month0 += 12;
    }

    // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
    let new_day = std::cmp::min::<u32>(
        day,
        last_day_of_year_month(new_year, (new_month0 + 1) as u32),
    );

    NaiveDate::from_ymd_opt(new_year, (new_month0 + 1) as u32, new_day).ok_or(format!(
        "Overflow on date YMD {}-{}-{}.",
        new_year,
        new_month0 + 1,
        new_day
    ))
}

// Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
fn last_day_of_year_month(year: i32, month: u32) -> u32 {
    let is_leap_year = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
    if std::intrinsics::unlikely(month == 2 && is_leap_year) {
        return 29;
    }
    let last_day_lookup = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    last_day_lookup[month as usize]
}

macro_rules! impl_interval_year_month {
    ($name: ident, $op: expr) => {
        #[derive(Clone)]
        pub struct $name;

        impl $name {
            pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> Result<i32, String> {
                let date = date.to_date(&Tz::UTC);
                let new_date = $op(date.year(), date.month(), date.day(), delta.as_());
                new_date.and_then(|d| {
                    check_date(
                        d.signed_duration_since(NaiveDate::from_ymd(1970, 1, 1))
                            .num_days(),
                    )
                })
            }

            pub fn eval_timestamp(ts: i64, delta: impl AsPrimitive<i64>) -> Result<i64, String> {
                let ts = ts.to_timestamp(&Tz::UTC);
                let new_ts = $op(ts.year(), ts.month(), ts.day(), delta.as_());
                new_ts.and_then(|t| {
                    check_timestamp(NaiveDateTime::new(t, ts.time()).timestamp_micros())
                })
            }
        }
    };
}

impl_interval_year_month!(AddYearsImpl, add_years_base);
impl_interval_year_month!(AddMonthsImpl, add_months_base);

pub struct AddDaysImpl;

impl AddDaysImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>) -> Result<i32, String> {
        check_date((date as i64).wrapping_add(delta.as_()))
    }

    pub fn eval_timestamp(date: i64, delta: impl AsPrimitive<i64>) -> Result<i64, String> {
        check_timestamp((date as i64).wrapping_add(delta.as_() * 24 * 3600 * 1_000_000))
    }
}

pub struct AddTimesImpl;

impl AddTimesImpl {
    pub fn eval_date(date: i32, delta: impl AsPrimitive<i64>, factor: i64) -> Result<i32, String> {
        check_date(
            (date as i64 * 3600 * 24 * 1_000_000).wrapping_add(delta.as_() * factor * 1_000_000),
        )
    }

    pub fn eval_timestamp(
        ts: i64,
        delta: impl AsPrimitive<i64>,
        factor: i64,
    ) -> Result<i64, String> {
        check_timestamp(ts.wrapping_add(delta.as_() * factor * 1_000_000))
    }
}
