SCHEDULER.every '5m', allow_overlapping: false do
  update_tick_es_w
  update_tick_uk_w
end

SCHEDULER.cron '1 0 * * * Europe/Paris' do
  update_daily_es_w
  update_daily_uk_w
end


SCHEDULER.cron '1 0 1 * * Europe/Paris' do
  update_monthly_es_w
  update_monthly_uk_w
end