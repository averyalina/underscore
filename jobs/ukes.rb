require('sequel')

$eu_targets = {"es" => 245, "uk" => 55 }

def get_eu_schema_name(country) 
  
  case country
  when 'es'
    schema = 'joliebox_es'
  when 'uk'
    schema = 'joliebox_uk'
  when 'fr'
    schema = 'jolie_box'
  end
  
  return schema
  
end 


def get_esuk_live_sub(country)

  schema = get_eu_schema_name(country) + "."

  
  strSubs = "SELECT duration, count(*) as num_subs
  FROM (

    -- active rebillables in current cycle
    SELECT s.duration, s.subscription_id, 0 AS boxes
    FROM " + schema + "jolie_sales_subscription_detail d
    JOIN " + schema +"jolie_sales_subscription s ON d.subscription_id = s.subscription_id
    LEFT JOIN
    (
      SELECT profile_id, sum(IF(`action` IN ('SUSPEND','reactivate'),IF(ACTION='SUSPEND',-1,+1),NULL)) AS final_action
      FROM " + schema +"jolie_sales_recurring_profile_history h
      WHERE `action` IN ('SUSPEND','reactivate') 
      GROUP BY h.profile_id
    ) st ON st.profile_id = s.recurring_profile_id
    WHERE d.box_sku = (SELECT VALUE FROM " + schema + "core_config_data WHERE path='jolie_cms/box/current_sku') AND payment_status IN ('paid_at','paid_at_time_of_order')
    AND s.duration  < 0 AND (st.final_action IS NULL OR st.final_action >= 0)

    UNION

     -- paid, active rebillables with box in next debit cycle but not this one! (probably in future cycle)
    SELECT s.duration, s.subscription_id, 0 AS boxes
    FROM " + schema + "jolie_sales_subscription_detail d
    JOIN " +schema + " jolie_sales_subscription s ON d.subscription_id = s.subscription_id
    LEFT JOIN
    (
      SELECT profile_id, sum(IF(`action` IN ('SUSPEND','reactivate'),IF(ACTION='SUSPEND',-1,+1),NULL)) AS final_action
      FROM " + schema + "jolie_sales_recurring_profile_history h
      WHERE `action` IN ('SUSPEND','reactivate') 
      GROUP BY h.profile_id
    ) st ON st.profile_id = s.recurring_profile_id
    WHERE d.box_sku = (SELECT VALUE FROM " + schema + "core_config_data WHERE path='jolie_sales/box/next_debit_sku') AND payment_status IN ('paid_at','paid_at_time_of_order')
    AND NOT EXISTS (SELECT 1 FROM " + schema + "jolie_sales_subscription_detail dd WHERE dd.subscription_id = d.subscription_id AND dd.payment_status IN ('paid_at','paid_at_time_of_order') AND dd.box_sku = (SELECT VALUE FROM " + schema +  "core_config_data WHERE path='jolie_cms/box/current_sku') AND payment_status IN ('paid_at','paid_at_time_of_order'))
    AND s.duration  < 0 AND (st.final_action IS NULL OR st.final_action >= 0) 



    UNION

    -- fixed duration subs with boxes remaining
    SELECT s.duration, s.subscription_id, count(*) AS boxes
    FROM " + schema + "jolie_sales_subscription_detail d
    JOIN " + schema + "jolie_sales_subscription s ON d.subscription_id = s.subscription_id
    WHERE s.duration  > 0 AND d.shipping_status IN ('shipping','shipped') AND LEFT(d.payment_status,4) = 'paid'
    GROUP BY s.subscription_id
    HAVING s.duration - count(*) > 0

    UNION

    -- fixed duration subs, paid, but never yet sent a box 
    SELECT s.duration, s.subscription_id, count(*) AS boxes
    FROM " + schema + "jolie_sales_subscription_detail d
    JOIN " + schema + "jolie_sales_subscription s ON d.subscription_id = s.subscription_id
    WHERE s.duration  > 0 AND  LEFT(d.payment_status,4) = 'paid'
    AND NOT EXISTS (SELECT 1 FROM " + schema + "jolie_sales_subscription_detail dd where dd.subscription_id = d.subscription_id AND  dd.shipping_status IN ('shipping','shipped'))
    GROUP BY s.subscription_id
    HAVING s.duration - count(*) > 0
  ) t
  GROUP BY duration;"
  
  rsSubs = DBEU.fetch(strSubs)
  return rsSubs.all.inject(0){|sum,e| sum += e[:num_subs] } 
end 









def get_eu_sub_delta_cumulative(period, country)
  
  schema = get_eu_schema_name(country) + "."
  
  case period
    when 1 #this month
      filter = "YEAR(s.created_at)=YEAR(now()) AND MONTH(s.created_at) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(s.created_at)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(s.created_at) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(s.created_at) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4 #today
      filter = "date(s.created_at) = date(now())"
  end  

  
  strQuery = "SELECT y,m, coalesce(group_concat(IF(gift_flag=1,qty,NULL)),0) AS num_gifts, coalesce(group_concat(IF(gift_flag=0,qty,NULL)),0) AS num_self
  FROM
  (
  	SELECT YEAR(s.created_at) AS y, MONTH(s.created_at) AS m, duration, g.giftcard_id IS NOT NULL AS gift_flag, count(*) AS qty
  	FROM " + schema + "jolie_sales_subscription s
  	LEFT JOIN " + schema +"jolie_giftcard g ON g.order_id = s.order_id
  	WHERE "  + filter +  "
  	GROUP BY y,m, gift_flag
  ) t
  GROUP BY y,m;
  "
   
  rsDelta = DBEU.fetch(strQuery)
  
  ngifts = 0
  nself = 0
  
  data = rsDelta.first
  
  unless data.nil? 
     ngifts += data[:num_gifts].to_i
     nself += data[:num_self].to_i
  end

  return {:ngift => ngifts, :nself =>nself}
end


def get_eu_orders_cumulative(period, country)
  
  
  schema = get_eu_schema_name(country) + "."
  
  case period
    when 1 #this month
      filter = "YEAR(o.created_at)=YEAR(now()) AND MONTH(o.created_at) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(o.created_at)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(o.created_at) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(o.created_at) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4
      filter = "date(o.created_at) = date(now())"
  end  

  
  strQuery = "SELECT count(DISTINCT i.order_id) AS num_orders
  FROM " + schema + "sales_flat_order o
  JOIN " + schema + "sales_flat_order_item i ON i.order_id = o.entity_id
  WHERE sku NOT LIKE '"+ country +"-JBX%' AND sku NOT LIKE 'GIFTCARD%' AND o.status IN ('processing','pending','complete','closed') AND " + filter + ";"
  
  rsOrders = DBEU.fetch(strQuery)
  
  norders = 0
    
  data = rsOrders.first
  
  unless data.nil? 
    norders = data[:num_orders]
  end

  return norders
end


def get_eu_cancellation_cumulative(period, country)
  
  schema = get_eu_schema_name(country) + "."
  
  case period
    when 1 #this month
      filter = "YEAR(created_at)=YEAR(now()) AND MONTH(created_at) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(created_at)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(created_at) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(created_at) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4
      filter = "date(created_at) = date(now())"
  end  

  
  strQuery = "SELECT count(*) as num
  FROM (
  SELECT profile_id, sum(IF(`action` IN ('SUSPEND','reactivate'),IF(ACTION='SUSPEND',-1,+1),NULL)) AS final_action
    FROM " + schema +  "jolie_sales_recurring_profile_history h
    WHERE `action` IN ('SUSPEND','reactivate') AND " + filter + "
    GROUP BY h.profile_id
  ) t
  WHERE final_action < 0"
  
  rsCancels = DBEU.fetch(strQuery)
  
  ncancels = 0
  
  data = rsCancels.first
  
  unless data.nil? 
      ncancels = data[:num]
  end

  return ncancels
end


#UK UPDATE FUNCTIONS
def update_monthly_uk_w
  #es womens
  delta_yday = get_eu_sub_delta_cumulative(2,'uk')
  send_event('sub_delta_uk_w_lmonth', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('cancel_uk_w_lmonth', { current: get_eu_cancellation_cumulative(2,'uk') })
  send_event('orders_uk_w_lmonth', { current: get_eu_orders_cumulative(2,'uk') })
end


def update_daily_uk_w
  #es womens
  delta_yday = get_eu_sub_delta_cumulative(3,'uk')
  send_event('sub_delta_uk_w_yday', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('orders_uk_w_yday', { current: get_eu_orders_cumulative(3,'uk') })
  send_event('cancel_uk_w_yday', { current: get_eu_cancellation_cumulative(3,'uk') })
end

def update_tick_uk_w
  
  send_event('sub_uk_w', {current: get_esuk_live_sub('uk')} )
  delta_tday = get_eu_sub_delta_cumulative(4,'uk')
  send_event('sub_delta_uk_w_tday', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  delta_tday = get_eu_sub_delta_cumulative(1,'es')
  send_event('sub_delta_uk_w_month', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  #orders
  send_event('orders_uk_w_month', { current: get_eu_orders_cumulative(1,'uk'), max: $eu_targets["uk"]  })
  send_event('orders_uk_w_today', { current: get_eu_orders_cumulative(4,'uk')})
  
  #cancellations
  send_event('cancel_uk_w_today', { current: get_eu_cancellation_cumulative(4,'uk') })
  send_event('cancel_uk_w_month', { current: get_eu_cancellation_cumulative(1,'uk') })
end


def update_all_uk_w
  update_tick_uk_w
  update_daily_uk_w
  update_monthly_uk_w
end


#ES UPDATE FUNCTIONS
def update_monthly_es_w
  #es womens
  delta_yday = get_eu_sub_delta_cumulative(2,'es')
  send_event('sub_delta_es_w_lmonth', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('cancel_es_w_lmonth', { current: get_eu_cancellation_cumulative(2,'es') })
  send_event('orders_es_w_lmonth', { current: get_eu_orders_cumulative(2,'es') })
end


def update_daily_es_w
  #es womens
  delta_yday = get_eu_sub_delta_cumulative(3,'es')
  send_event('sub_delta_es_w_yday', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('orders_es_w_yday', { current: get_eu_orders_cumulative(3,'es') })
  send_event('cancel_es_w_yday', { current: get_eu_cancellation_cumulative(3,'es') })
end

def update_tick_es_w
  
  send_event('sub_es_w', {current: get_esuk_live_sub('es')} )
  delta_tday = get_eu_sub_delta_cumulative(4,'es')
  send_event('sub_delta_es_w_tday', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  delta_tday = get_eu_sub_delta_cumulative(1,'es')
  send_event('sub_delta_es_w_month', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  #orders
  send_event('orders_es_w_month', { current: get_eu_orders_cumulative(1,'es'), max: $eu_targets["es"] })
  send_event('orders_es_w_today', { current: get_eu_orders_cumulative(4,'es') })
  
  #cancellations
  send_event('cancel_es_w_today', { current: get_eu_cancellation_cumulative(4,'es') })
  send_event('cancel_es_w_month', { current: get_eu_cancellation_cumulative(1,'es') })
end


def update_all_es_w
  update_tick_es_w
  update_daily_es_w
  update_monthly_es_w
end


update_all_es_w
update_all_uk_w
