# ETL Orders

## Исходные данные

```csv
order_id,customer_id,order_ts,status,amount,is_deleted,updated_at
1001,11,2026-03-01 10:00:00,paid,1200.50,false,2026-03-01 10:05:00
1002,12,2026-03-01 10:20:00,new,450.00,false,2026-03-01 10:21:00
1003,11,2026-03-01 11:00:00,paid,980.00,false,2026-03-01 11:01:00
1004,13,2026-03-01 11:30:00,cancelled,300.00,false,2026-03-01 11:31:00
1005,14,2026-03-01 12:00:00,paid,2100.00,false,2026-03-01 12:02:00
```

## Добавлены после full-load

```csv
1002,12,2026-03-01 10:20:00,paid,450.00,false,2026-03-01 12:30:00
1006,15,2026-03-01 12:45:00,new,760.00,True,2026-03-01 12:46:00
1007,16,2026-03-01 13:10:00,paid,1890.00,false,2026-03-01 13:11:00
```

## Логи двух запусков с проверкой качества

### full-load 

```text
QUALITY CHECKS
null_critical: 0
duplicate_orders: []
non_positive_amount: 0
All checks passed

MART ORDERS DAILY
    order_day  orders_cnt  total_revenue
0  2026-03-01           4         4730.5
```

### incremental-load 

```text
QUALITY CHECKS
null_critical: 0
duplicate_orders: []
non_positive_amount: 0
All checks passed

MART ORDERS DAILY
    order_day  orders_cnt  total_revenue
0  2026-03-01           5         6620.5
```

## Снимок `dwh_orders`

```text
   order_id  customer_id             order_ts     status  amount  is_deleted           updated_at            load_dttm
0      1001           11  2026-03-01 10:00:00       paid  1200.5           0  2026-03-01 10:05:00  2026-04-23 13:44:17
1      1002           12  2026-03-01 10:20:00       paid   450.0           0  2026-03-01 12:30:00  2026-04-23 13:45:21
2      1003           11  2026-03-01 11:00:00       paid   980.0           0  2026-03-01 11:01:00  2026-04-23 13:44:17
3      1004           13  2026-03-01 11:30:00  cancelled   300.0           0  2026-03-01 11:31:00  2026-04-23 13:44:17
4      1005           14  2026-03-01 12:00:00       paid  2100.0           0  2026-03-01 12:02:00  2026-04-23 13:44:17
5      1006           15  2026-03-01 12:45:00        new   760.0           1  2026-03-01 12:46:00  2026-04-23 13:45:21
6      1007           16  2026-03-01 13:10:00       paid  1890.0           0  2026-03-01 13:11:00  2026-04-23 13:45:21
```

## Снимок `etl_state`

```text
     pipeline_name      last_success_ts
0  orders_pipeline  2026-03-01 13:11:00
```
