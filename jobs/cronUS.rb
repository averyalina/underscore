SCHEDULER.every '5m', allow_overlapping: false do
  update_tick_us_w
  update_tick_us_m
  update_tick_us_mixed
end

SCHEDULER.cron '1 0 * * * America/New_York' do
  update_daily_us_w
  update_daily_us_m
  update_daily_us_mixed
end


SCHEDULER.cron '1 0 1 * * America/New_York' do
  update_monthly_us_w
  update_monthly_us_m
  update_monthly_us_mixed
end