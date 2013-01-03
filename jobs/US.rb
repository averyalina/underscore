require('sequel')

def get_us_live_sub(vertical_id)
  strSubs = "SELECT subscription_vertical_id as vertical_id, subscription_type = 4 AS gift_flag, count(*) as num_subs
  FROM subscriptions s 
  JOIN customer_subscription_profile p ON p.id = s.customer_subscription_profile_id
  WHERE 
  -- get only one active subscription per profile
  s.id = (
      SELECT s1.id
      FROM subscriptions_view s1
      LEFT JOIN rebillable_subscriptions r ON r.rebillable_id = s1.id AND s1.subscription_type = 3
      LEFT JOIN gift_subscriptions g ON g.gift_id = s1.id  AND s1.subscription_type = 4
      JOIN customer_subscription_profile p
      ON p.id = s1.customer_subscription_profile_id 
      JOIN subscription_type_priority pri
      ON pri.subscription_type = s1.subscription_type

      WHERE IF((`s`.`subscription_type` = 3),IF((`s`.`status` = 0),0,IF((`r`.`billing_status` IN (1)),1,0)),IF((`s`.`subscription_type` = 4),IF((`s`.`status` = 0),0,IF((`g`.`gift_status` IN (2,5)),1,0)),`s`.`status`)) = 1
      AND s1.customer_subscription_profile_id = s.customer_subscription_profile_id

      ORDER BY pri.priority, s1.boxes_remaining
      LIMIT 1
  )
  
  GROUP BY subscription_vertical_id, subscription_type = 4;"
  
  rsSubs = DB.fetch(strSubs)
  data = rsSubs.all
  
  ngifts = 0
  nself = 0
  
  
  unless data.nil? 
    data.each do |result|
      if result[:vertical_id]==vertical_id
        if result[:gift_flag]==1
          ngifts += result[:num_subs]
        else 
          nself += result[:num_subs]
        end
      end
    end
  end
    
  return {:ngift => ngifts, :nself =>nself} 
end


def get_us_orders_cumulative(period, order_type)

  case period
    when 1 #this month
      filter = "YEAR(date_sub(created_at, INTERVAL 5 HOUR))=YEAR(now()) AND MONTH(date_sub(created_at, INTERVAL 5 HOUR)) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(date_sub(created_at, INTERVAL 5 HOUR))=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(date_sub(created_at, INTERVAL 5 HOUR)) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(date_sub(created_at, INTERVAL 5 HOUR)) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4 #today
      filter = "date(date_sub(created_at, INTERVAL 5 HOUR))= date(now())"
  end

  strOrders = "  SELECT YEAR(date_sub(created_at, INTERVAL 5 HOUR)) AS y, MONTH(date_sub(created_at, INTERVAL 5 HOUR)) AS m, order_type, count(*) AS num_orders
    FROM (
    SELECT o.entity_id, o.created_at, group_concat(DISTINCT i.store_id ORDER BY i.store_id) AS order_type
    FROM sales_flat_order o
    JOIN sales_flat_order_item i ON i.order_id = o.entity_id
    WHERE i.product_type IN ('simple','configurable') AND i.price > 0 AND i.store_id IS NOT NULL AND o.status IN ('complete', 'processing')
    GROUP BY o.entity_id
    ) t
    WHERE "  +  filter + "
    GROUP BY order_type;"  
    
    norders = 0
    
    data = DB.fetch(strOrders).all
    
    unless data.nil?
      orders = data.select { |v| v[:order_type]==order_type }
      unless orders.first.nil?  
        norders += orders.first[:num_orders] 
      end
    end
    
    return norders

end


def get_us_sub_delta_cumulative(period, vertical_id)

  case period
  when 1 #this month
    filter = "YEAR(date_sub(s.created_at, INTERVAL 5 HOUR))=YEAR(now()) AND MONTH(date_sub(s.created_at, INTERVAL 5 HOUR)) = MONTH(now())" 
  when 2 #last month
    filter = "YEAR(date_sub(s.created_at, INTERVAL 5 HOUR))=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(date_sub(s.created_at, INTERVAL 5 HOUR)) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
  when 3 #yesterday
    filter = "date(date_sub(s.created_at, INTERVAL 5 HOUR)) = date(date_sub(now(), INTERVAL 1 DAY))"
  when 4 #today
    filter = "date(date_sub(s.created_at, INTERVAL 5 HOUR))= date(now())"
  end

  filter +=  " AND subscription_vertical_id = " + vertical_id.to_s

  strQuery = "SELECT p.subscription_vertical_id AS vertical_id, subscription_type, count(*) AS num_subs 
  FROM subscriptions_view s
  JOIN customer_subscription_profile p ON p.id = s.customer_subscription_profile_id
  WHERE s.subscription_type IN (3,4) AND s.is_active = 1 AND " + filter + 
  " GROUP BY p.subscription_vertical_id, subscription_type;"


  data = DB.fetch(strQuery).all

  ngifts = 0
  nself = 0

  unless data.nil? 
    data.each do |v| 
      if(v[:subscription_type] == 3) then 
        ngifts += v[:num_subs]
      else 
        nself  += v[:num_subs]
      end
    end 
  end

  return {:ngift => ngifts, :nself =>nself}
end



def get_us_cancellation_cumulative(period, vertical_id)

  case period
    when 1 #this month
      filter = "YEAR(date_sub(x.created_at, INTERVAL 5 HOUR))=YEAR(now()) AND MONTH(date_sub(x.created_at, INTERVAL 5 HOUR)) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(date_sub(x.created_at, INTERVAL 5 HOUR))=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(date_sub(x.created_at, INTERVAL 5 HOUR)) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(date_sub(x.created_at, INTERVAL 5 HOUR)) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4 #today
      filter = "date(date_sub(x.created_at, INTERVAL 5 HOUR))= date(now())"
  end

  filter +=  " AND subscription_vertical_id = " + vertical_id.to_s

  strQuery = "SELECT count(*) AS num_cancels
  FROM
  	(SELECT s.id
  	FROM subscriptions_cancellation x
  	JOIN subscriptions s ON x.subscription_id = s.id
  	JOIN customer_subscription_profile p ON p.id = s.customer_subscription_profile_id
  	WHERE x.`cancellation_reason` <> 209 AND " + filter + " 
  	GROUP BY s.id) t;"

  ncancels = 0
  data = DB.fetch(strQuery).first



  unless data.nil? 
    ncancels = data[:num_cancels] 
  end

  return ncancels
end


def get_us_list_joins_cumulative(period, vertical_id)

  case period
  when 1 #this month
    filter = "YEAR(date_sub(x.created_at, INTERVAL 5 HOUR))=YEAR(now()) AND MONTH(date_sub(x.created_at, INTERVAL 5 HOUR)) = MONTH(now())" 
  when 2 #last month
    filter = "YEAR(date_sub(x.created_at, INTERVAL 5 HOUR))=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(date_sub(x.created_at, INTERVAL 5 HOUR)) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
  when 3 #yesterday
    filter = "date(date_sub(x.created_at, INTERVAL 5 HOUR)) = date(date_sub(now(), INTERVAL 1 DAY))"
  when 4 #today
    filter = "date(date_sub(x.created_at, INTERVAL 5 HOUR))= date(now())"
  end

  strQuery = "SELECT count(DISTINCT email) AS num_list_joins
  FROM invitations as x
  JOIN waitlists as y
  ON x.waitlist_id = y.id
  WHERE x.waitlist_id in (11,16)
  AND " + filter + "
  AND y.subscription_vertical_id = " + vertical_id.to_s + ";"

  nlistjoins = 0
  data = DB.fetch(strQuery).first

  unless data.nil?
    nlistjoins = data[:num_list_joins]
  end

  return nlistjoins
end

#US WOMENS

def update_monthly_us_w
  #us womens
  delta_yday = get_us_sub_delta_cumulative(2,1)
  send_event('sub_delta_us_w_lmonth', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('cancel_us_w_lmonth', { current: get_us_cancellation_cumulative(2,"1") })
  send_event('orders_us_w_lmonth', { current: get_us_orders_cumulative(2,"1") })
  send_event('list_joins_us_w_lmonth', { current: get_us_list_joins_cumulative(2,1) })
end


def update_daily_us_w
  #us womens
  delta_yday = get_us_sub_delta_cumulative(3,1)
  send_event('sub_delta_us_w_yday', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('orders_us_w_yday', { current: get_us_orders_cumulative(3,"1") })
  send_event('cancel_us_w_yday', { current: get_us_cancellation_cumulative(3,1) })
  send_event('list_joins_us_w_yday', { current: get_us_list_joins_cumulative(3,1) })
end

def update_tick_us_w
  
  live_subs =  get_us_live_sub(1)
  
  send_event('sub_us_w', {current: live_subs[:ngift] + live_subs[:nself]})
  delta_tday = get_us_sub_delta_cumulative(4,1)
  send_event('sub_delta_us_w_tday', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  delta_tday = get_us_sub_delta_cumulative(1,1)
  send_event('sub_delta_us_w_month', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  #orders
  send_event('orders_us_w_month', { current: get_us_orders_cumulative(1,"1") })
  send_event('orders_us_w_today', { current: get_us_orders_cumulative(4,"1") })
  
  #cancellations
  send_event('cancel_us_w_today', { current: get_us_cancellation_cumulative(4,1) })
  send_event('cancel_us_w_month', { current: get_us_cancellation_cumulative(1,1) })

  #list joins
  send_event('list_joins_us_w_today', { current: get_us_list_joins_cumulative(4,1) })
  send_event('list_joins_us_w_month', { current: get_us_list_joins_cumulative(1,1) })
end


def update_all_us_w
  update_tick_us_w
  update_daily_us_w
  update_monthly_us_w
end


#US MENS

def update_monthly_us_m
  #us mens
  delta_yday = get_us_sub_delta_cumulative(2,2)
  send_event('sub_delta_us_m_lmonth', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('cancel_us_m_lmonth', { current: get_us_cancellation_cumulative(2,2) })
  send_event('orders_us_m_lmonth', { current: get_us_orders_cumulative(2,"4") })
  send_event('list_joins_us_m_lmonth', { current: get_us_list_joins_cumulative(2,2) })
end


def update_daily_us_m
  #us mens
  delta_yday = get_us_sub_delta_cumulative(3,1)
  send_event('sub_delta_us_m_yday', {num1: delta_yday[:nself], num2: delta_yday[:ngift] })
  send_event('orders_us_m_yday', { current: get_us_orders_cumulative(3,"4") })
  send_event('cancel_us_m_yday', { current: get_us_cancellation_cumulative(3,2) })
  send_event('list_joins_us_m_yday', { current: get_us_list_joins_cumulative(3,2) })
end

def update_tick_us_m
  
  live_subs =  get_us_live_sub(2)
  
  send_event('sub_us_m', {current: live_subs[:ngift] + live_subs[:nself]})
  delta_tday = get_us_sub_delta_cumulative(4,2)
  send_event('sub_delta_us_m_tday', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  delta_tday = get_us_sub_delta_cumulative(1,2)
  send_event('sub_delta_us_m_month', {num1: delta_tday[:nself], num2: delta_tday[:ngift] })
  
  #orders
  send_event('orders_us_m_month', { current: get_us_orders_cumulative(1,"4") })
  send_event('orders_us_m_today', { current: get_us_orders_cumulative(4,"4") })
  
  #cancellations
  send_event('cancel_us_m_today', { current: get_us_cancellation_cumulative(4,2) })
  send_event('cancel_us_m_month', { current: get_us_cancellation_cumulative(1,2) })

  #list joins
  send_event('list_joins_us_m_today', {current: get_us_list_joins_cumulative(4,2) })
  send_event('list_joins_us_m_month', {current: get_us_list_joins_cumulative(1,2) })
end


def update_all_us_m
  update_tick_us_m
  update_daily_us_m
  update_monthly_us_m
end


#MIXED ORDERS

def update_tick_us_mixed
  #orders
  send_event('orders_us_mixed_month', { current: get_us_orders_cumulative(1,"1,4") })
  send_event('orders_us_mixed_today', { current: get_us_orders_cumulative(4,"1,4") })
end


def update_monthly_us_mixed
  send_event('orders_us_mixed_lmonth', { current: get_us_orders_cumulative(2,"1,4") })
end


def update_daily_us_mixed
  send_event('orders_us_mixed_yday', { current: get_us_orders_cumulative(3,"1,4") })
end

def update_all_us_mixed
  update_tick_us_mixed
  update_monthly_us_mixed
  update_daily_us_mixed
end 

update_all_us_w
update_all_us_m
update_all_us_mixed
