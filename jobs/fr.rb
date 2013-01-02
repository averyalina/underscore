require('sequel')

def get_fr_live_sub
  
  strSubs = "  SELECT (SELECT id FROM wp_jb_boxes WHERE stock > 0 AND shipping_status_id < 3 LIMIT 0,1) AS box_id, quantity, count(*) AS num_subs
  FROM (

    -- active rebillables 
    SELECT d.quantity, d.id, NULL AS boxes_sent
    FROM wp_jb_orders o
    JOIN wp_jb_order_details d ON o.id = d.order_id 
    JOIN wp_jb_order_detail_sub s ON s.order_detail_id = d.id AND s.box_id = (SELECT id FROM wp_jb_boxes WHERE stock > 0 AND shipping_status_id < 3 LIMIT 0,1)
    WHERE d.`quantity` < 0 AND d.sub_active = 'YES' AND o.status_id IN (1,3) AND s.sub_payment_status_id NOT IN (7)

    UNION

    -- unexpired fixed duration subs who have been shipped
    SELECT d.quantity, d.id, count(s.id) AS boxes_sent
    FROM wp_jb_orders o
    JOIN wp_jb_order_details d ON o.id = d.order_id
    JOIN wp_jb_order_detail_sub s ON s.order_detail_id = d.id
    WHERE d.`quantity` > 0 AND s.shipping_status_id IN (4,5) AND o.status_id IN (1,3)
    GROUP BY d.id
    HAVING quantity - count(s.id) > 0

    UNION

    -- fixed duration subs who will ship in the future, but who have not shipped any box yet!
    SELECT d.quantity, d.id, count(s.id) AS boxes_sent
    FROM wp_jb_orders o
    JOIN wp_jb_order_details d ON o.id = d.order_id
    JOIN wp_jb_order_detail_sub s ON s.order_detail_id = d.id
    WHERE d.`quantity` > 0 AND d.sub_active = 'YES' AND s.shipping_status_id IN (1,2) AND o.status_id IN (1,3)
    AND NOT EXISTS (SELECT 1 FROM wp_jb_order_detail_sub ss WHERE ss.order_detail_id = d.id AND ss.shipping_status_id IN (4,5) )
    GROUP BY d.id
    HAVING quantity - count(s.id) > 0

    ) t
  GROUP BY quantity;"

  rsSubs = DBEU.fetch(strSubs)
  return rsSubs.all.inject(0){|sum,e| sum += e[:num_subs] }

end

def get_fr_sub_delta_cumulative(period)
  
  case period
    when 1 #this month
      filter = "YEAR(`date`)=YEAR(now()) AND MONTH(`date`) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(`date`)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(`date`) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(`date`) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4 #today
      filter = "date(`date`) = date(now())"
  end  

  
  strQuery = "SELECT coalesce(group_concat(IF(quantity=1 AND gift_flag=1,ct,NULL)),0) AS gift_1_m, coalesce(group_concat(IF(quantity=3 AND gift_flag=1,ct,NULL)),0) AS gift_3_m, coalesce(group_concat(IF(quantity=6 AND gift_flag=1,ct,NULL)),0) AS gift_6_m, coalesce(group_concat(IF(quantity=12 AND gift_flag=1,ct,NULL)),0) AS gift_12_m, coalesce(group_concat(IF(quantity=1 AND gift_flag=0,ct,NULL)),0) AS 1_m, coalesce(group_concat(IF(quantity=3 AND gift_flag=0,ct,NULL)),0) AS 3_m, coalesce(group_concat(IF(quantity=6 AND gift_flag=0,ct,NULL)),0) AS 6_m, coalesce(group_concat(IF(quantity=12 AND gift_flag=0,ct,NULL)),0) AS 12_m, coalesce(group_concat(IF(quantity=-1 AND gift_flag=0,ct,NULL)),0) AS rebillable_m, coalesce(group_concat(IF(quantity=-12 AND gift_flag=0,ct,NULL)),0) AS rebillable_y
  FROM (
  SELECT YEAR(DATE) AS y, MONTH(DATE) AS m, concat(YEAR(DATE),'-',MONTH(DATE)) AS period_start,  quantity, gift_card_id <> 0 AS gift_flag, count(*) AS ct
  FROM wp_jb_orders o
  JOIN wp_jb_order_details d ON o.id = d.order_id
  WHERE d.type = 'SUB' AND o.user_id <> 1 AND o.status_id IN (1,3) AND " + filter + " " +
  "GROUP BY y, m, quantity, gift_card_id <> 0
  ) t
  GROUP BY y,m;"
  
  rsDelta = DBEU.fetch(strQuery)
  
  ngifts = 0
  nself = 0
  
  data = rsDelta.first
  
  unless data.nil? 
    data.each {|k,v| if(k[0..3] == 'gift') 
                        then ngifts += v.to_i
                        else nself += v.to_i
                      end }
  end

  return {:ngift => ngifts, :nself =>nself}
end


def get_fr_orders_cumulative(period)
  
  case period
    when 1 #this month
      filter = "YEAR(`date`)=YEAR(now()) AND MONTH(`date`) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(`date`)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(`date`) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(`date`) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4
      filter = "date(`date`) = date(now())"
  end  

  
  strQuery = "SELECT count(DISTINCT o.id) AS num_orders
  FROM wp_jb_orders o 
  JOIN wp_jb_order_details d ON o.id = d.order_id
  WHERE  o.status_id IN (1,3) AND d.type = 'PROD' AND " + filter + ";"
  
  rsOrders = DBEU.fetch(strQuery)
  
  norders = 0
    
  data = rsOrders.first
  
  unless data.nil? 
    norders = data[:num_orders]
  end

  return norders
end


def get_fr_cancellation_cumulative(period)
  
  case period
    when 1 #this month
      filter = "YEAR(h.timestamp)=YEAR(now()) AND MONTH(h.timestamp) = MONTH(now())" 
    when 2 #last month
      filter = "YEAR(h.timestamp)=YEAR(date_sub(now(), INTERVAL 1 MONTH)) AND MONTH(h.timestamp) = MONTH(date_sub(now(), INTERVAL 1 MONTH))" 
    when 3 #yesterday
      filter = "date(h.timestamp) = date(date_sub(now(), INTERVAL 1 DAY))"
    when 4
      filter = "date(h.timestamp) = date(now())"
  end  

  
  strQuery = "SELECT final_action<0 as type, count(*) as num
  FROM (
    SELECT d.order_id, sum(h.action) AS final_action
    FROM wp_jb_sub_history h
    JOIN wp_jb_order_details d ON d.id = h.order_detail_id
    WHERE " + filter + " 
    GROUP BY d.order_id
  ) t
  GROUP BY final_action<0;"
  
  rsCancels = DBEU.fetch(strQuery)
  
  ncancels = 0
    
  unless rsCancels.all.nil? or rsCancels.all.index{ |i| i[:type]==1 }.nil?
      data = rsCancels.all.fetch(rsCancels.all.index{ |i| i[:type]==1 })
      ncancels = data[:num]
  end

  return ncancels
end



def update_monthly 
  #france
  fr_delta_yday = get_fr_sub_delta_cumulative(2)
  send_event('sub_delta_fr_lmonth', {num1: fr_delta_yday[:nself], num2: fr_delta_yday[:ngift] })
  send_event('cancel_fr_lmonth', { current: get_fr_cancellation_cumulative(2) })
end


def update_daily
  #france
  fr_delta_yday = get_fr_sub_delta_cumulative(3)
  send_event('sub_delta_fr_yday', {num1: fr_delta_yday[:nself], num2: fr_delta_yday[:ngift] })
  send_event('orders_fr_yday', { current: get_fr_orders_cumulative(3) })
  send_event('cancel_fr_yday', { current: get_fr_cancellation_cumulative(3) })
end


def update_fr
  send_event('sub_fr', {current: get_fr_live_sub})
  fr_delta_tday = get_fr_sub_delta_cumulative(4)
  send_event('sub_delta_fr_tday', {num1: fr_delta_tday[:nself], num2: fr_delta_tday[:ngift] })
  
  fr_delta_tday = get_fr_sub_delta_cumulative(1)
  send_event('sub_delta_fr_month', {num1: fr_delta_tday[:nself], num2: fr_delta_tday[:ngift] })
  
  #orders
  send_event('orders_fr_month', { current: get_fr_orders_cumulative(1) })
  send_event('orders_fr_today', { current: get_fr_orders_cumulative(4) })
  
  #cancellations
  send_event('cancel_fr_today', { current: get_fr_cancellation_cumulative(4) })
  send_event('cancel_fr_month', { current: get_fr_cancellation_cumulative(1) })
end

def update_all
  update_fr
end

update_all
update_fr
update_monthly
update_daily


SCHEDULER.every '5m', allow_overlapping: false do
  update_all
end

SCHEDULER.cron '1 0 * * * Europe/Paris' do
  update_daily
end


SCHEDULER.cron '1 0 1 * * Europe/Paris' do
  update_monthly
end
