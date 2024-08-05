#!/bin/bash

APP='car_data'

if [ -n "$2" ]; then
    do_date=$2
else
    do_date=$(date -d '-1 day' +%F)
fi

dws_electricity_single_trip_detail="
insert overwrite table ${APP}.dws_electricity_single_trip_detail partition (dt='${do_date}')
select
    concat(vin,'-',min(\`timestamp\`)) id,
    vin,
    min(\`timestamp\`) start_timestamp,
    max(\`timestamp\`)end_timestamp,
    min(mileage) start_mileage,
    max(mileage)end_mileage,
    max(soc) start_soc,
    min(soc) end_soc,
    avg(velocity) avg_speed,
    avg(voltage) car_avg_voltage,
    avg(electric_current) car_avg_electric_current,
    avg(max_temperature) battery_avg_max_temperature,
    collect_list(max_temperature)[cast(count(*)/2 as int)]  battery_mid_max_temperature,
    avg(min_temperature) battery_avg_min_temperature,
    collect_list(min_temperature)[cast(count(*)/2 as int)] battery_mid_min_temperature,
    avg(max_voltage) battery_avg_max_voltage,
    collect_list(max_voltage)[cast(count(*)/2 as int)] battery_mid_max_voltage,
    avg(min_voltage) battery_avg_min_voltage,
    collect_list(min_voltage)[cast(count(*)/2 as int)] battery_mid_min_voltage
from (
     select
        vin,
        velocity,
        mileage,
        voltage,
        electric_current,
        soc,
        max_temperature,
        max_voltage,
        min_temperature,
        min_voltage,
        \`timestamp\`,
        dt,
        sum(mark)over (partition by vin order by \`timestamp\`) singer_trip
    from (
         select
            vin,
            velocity,
            mileage,
            voltage,
            electric_current,
            soc,
            max_temperature,
            max_voltage,
            min_temperature,
            min_voltage,
            \`timestamp\`,
            dt,
            if((lag(\`timestamp\`,1,0)over (partition by vin order by \`timestamp\` ) - \`timestamp\`) < -60000,1,0) mark
        from ${APP}.dwd_car_running_electricity_inc
        where dt='${do_date}'
    )t1
)t2
group by vin,singer_trip;
"

dws_single_charge_detail_inc="insert overwrite table ${APP}.dws_single_charge_detail_inc partition (dt='2023-05-03')
select
    concat(vin,'-',min(\`timestamp\`)) id,
    vin,
    min(\`timestamp\`) start_timestamp,
    max(\`timestamp\`)end_timestamp,
    max(soc) start_soc,
    min(soc) end_soc,
    max(voltage) max_total_voltage,
    min(voltage) min_total_voltage,
    avg(voltage) avg_total_voltage,
    max(electric_current) max_current,
    min(electric_current) min_current,
    avg(electric_current) avg_current,
    avg(max_temperature) battery_avg_max_temperature,
    collect_list(max_temperature)[cast(count(*)/2 as int)]  battery_mid_max_temperature,
    avg(min_temperature) battery_avg_min_temperature,
    collect_list(min_temperature)[cast(count(*)/2 as int)] battery_mid_min_temperature,
    avg(max_voltage) battery_avg_max_voltage,
    collect_list(max_voltage)[cast(count(*)/2 as int)] battery_mid_max_voltage,
    avg(min_voltage) battery_avg_min_voltage,
    collect_list(min_voltage)[cast(count(*)/2 as int)] battery_mid_min_voltage
from (
     select
        vin,
        voltage,
        electric_current,
        soc,
        max_temperature,
        max_voltage,
        min_temperature,
        min_voltage,
        \`timestamp\`,
        dt,
        sum(mark)over (partition by vin order by \`timestamp\`) singer_trip
    from (
         select
            vin,
            voltage,
            electric_current,
            soc,
            max_temperature,
            max_voltage,
            min_temperature,
            min_voltage,
            \`timestamp\`,
            dt,
            if((lag(\`timestamp\`,1,0)over (partition by vin order by \`timestamp\` ) - \`timestamp\`) < -60000,1,0) mark
        from ${APP}.dwd_car_parking_charging_inc
        where dt='${do_date}'
    )t1
)t2
group by vin,singer_trip;
"

case $1 in
'dws_electricity_single_trip_detail')
    hive -e "$dws_electricity_single_trip_detail"
    ;;
'dws_single_charge_detail_inc')
    hive -e "$dws_single_charge_detail_inc"
    ;;
"all")
    hive -e "$dws_electricity_single_trip_detail$dws_single_charge_detail_inc"
    ;;
esac