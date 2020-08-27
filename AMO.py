from clickhouse_driver import Client
import datetime
import os


client = Client(
    host=os.getenv("CH_HOST"),
    user=os.getenv("CH_USER_W"),
    password=os.getenv("CH_PASSWORD_W"),
    port=os.getenv("CH_PORT"),
    database='fl'
)

import datetime

today = os.getenv('ds')
today_ok = datetime.date(int(today.split('-')[0]),int(today.split('-')[1]),int(today.split('-')[2])) #пребразование строки в дату
this_monday = (today_ok + datetime.timedelta(days=-today_ok.weekday()))
before_monday = (this_monday - datetime.timedelta(7)) #понедельник прошлой недели
before_seven = (before_monday + datetime.timedelta(6)) #воскресенье прошлой недели
#print (today_ok, this_monday,before_monday,before_seven)

sql = '''INSERT INTO fl_test.final_funnel_test
       select
        old_status_id
        ,lead_id
        ,pipeline_id
        ,upd
        ,created_at_dt
        ,voronka
        ,status_name
        ,name_lead
        ,tags
        ,custom_fields
        ,if (toMonday(upd)=toMonday(created_at_dt), 'down_funnel', 'up_funnel') as type_funnel
        ,splitByChar('"',custom_fields) [54] as reson_refuse
        ,toDate('{before_monday}') as dt                            ---Меняем на понедельники
    from
    (
            (
    select
            old_status_id
            ,lead_id
            ,pipeline_id
            ,upd
            ,created_at_dt
            ,voronka
            ,status_name
            ,name_lead
            ,tags
            ,'' as custom_fields
            from
            (
            select
            old_status_id
            ,toInt32OrNull(id) as lead_id
            ,toInt32OrNull(pipeline_id) as pipeline_id
            ,upd
            ,created_at_d as created_at_dt
            ,toInt32OrNull(arrayJoin(synt_arr)) as voronka
            from
            (
            --смотрим какой по счету был old_status_id в массиве он был третьим
            select
            *
            ,indexOf(status_array, old_status_id) as ind
            ,arraySlice(status_array, 1, ind) as synt_arr
            from
            (
            --Берем id лида из FL на этом этапе все ок лид 20181581 есть в воронке
            select  id
                    ,pipeline_id
                    , argMax (status_id, updated_at_dt) as old_status_id
                    , argMax (created_at_dt, updated_at_dt) as created_at_d
                    , max (updated_at_dt) as upd
                    --, ts_captured
                 from
                    --т.к. встечаются случаи когда идет 3 изменения статуа в 1 сек id=20181581 то чтобы правильно работал аргмакс были убраны
                    --условные дубли
                    (
                    select  distinct id ,pipeline_id ,updated_at,updated_at_dt,created_at_dt
                    , argMax (status_id, toInt64OrNull(concat (updated_at,status_sort))) as status_id
                    , max (toInt64OrNull(concat (updated_at,status_sort))) as concat
                        from talenttech.stt_amocrm_lead_change_mt ld left join talenttech.amocrm_tt_funnels_statuses st on ld.status_id =st.status_id
                        where pipeline_id='2289430'  --and id ='20181581'
                        and toInt32OrNull (status_sort)<=10000
                        group by id ,pipeline_id ,updated_at,updated_at_dt,created_at_dt
                        order by updated_at_dt
                    ) m
                    --т.к. встечаются случаи когда идет 3 изменения статуа в 1 сек id=20181581 то чтобы правильно работал аргмакс были убраны
                    --условные дубли
            where toInt32(status_id)!=143
              --and toDate(updated_at_dt)<='2020-07-19'
               and pipeline_id='2289430'
               and toDate (created_at_dt) between  '{before_monday}' and '{before_seven}'	 ----меняем недели
               and toDate (updated_at_dt) between  '{before_monday}' and '{before_seven}'	 ----меняем недели
                              ---выираем только те лиды, которые созданы в указанный период
            group by id,pipeline_id
            --Берем id лида из FL на этом этапе все ок есть в воронке
            ) l
            left join
                (
                --оборачиваем в массив
                select id
                      ,groupArray(status_id) as status_array
                from
                    --берем все id статусов и сортируем от боьшего к меньшему
                    (
                      select id
                            ,status_id
                        from talenttech.amocrm_tt_funnels_statuses
                       where id=2289430
                    order by toInt32(status_sort)
                    )
                    --берем все id статусов и сортируем от боьшего к меньшему
                group by id
                --оборачиваем в массив
                ) ar
            on l.pipeline_id=toString(ar.id)
            --смотрим какой по счету был old_status_id в массиве он был третьим
            ) g
            ) final_table
            left join talenttech.amocrm_tt_funnels_statuses status on voronka= toInt32OrNull(status.status_id)
            left join
    --убрал пайплайн т.к. сделки могли еренесни в другую воронку. не на что не влияет, тольль если сделка перенесена. то имя остается, в против. нет
            (
                select
                        id as l_id
                        ,name as name_lead
                        ,tags
                  from talenttech.amocrm_tt_leads
                -- where pipeline_id =2289430
                ---where  l_id=20409893
                 ) g2
    --убрал пайплайн т.к. сделки могли еренесни в другую воронку. не на что не влияет, тольль если сделка перенесена. то имя остается, в против. нет
                on l_id=lead_id
             where  status.id =2289430
               and (tags not like '%Лендинг%' or tags  not like '%Заявка с сайта%')  --условие на лиды где нет в тегах Лендинг и Заявка с сайта
               )
    union all
    ---делаем закрыто и нереализовано
          (
           select  ok.old_status_id as old_status_id
                    ,toInt32OrNull(ok.id) as  lead_id
                    ,toInt32OrNull(ok.pipeline_id)  as pipeline_id
                    ,ok.upd as upd
                    ,ok.created_at_d as created_at_dt
                    ,143 as voronka
                    ,'Закрыто и не реализовано' as status_name
                    ,ok.name  as name_lead
                    ,tags
                    , custom_fields
            from
            --получилось 38 а не 37 из за 21758379 лида
             (
            select	id
                    ,name_lead as name
                    ,pipeline_id
                    , old_status_id
                    , created_at_d
                    , upd
                    , custom_fields
              from
                    (
                            select  id
                                    ,name
                                    ,pipeline_id
                                    , custom_fields
                                    , argMax (status_id, updated_at_dt) as old_status_id
                                    , argMax (created_at_dt, updated_at_dt) as created_at_d
                                    , max (updated_at_dt) as upd
                             from talenttech.stt_amocrm_lead_change_mt
                            where toInt32(status_id)=143
                              and pipeline_id='2289430'
                              and toDate (created_at_dt) between  '{before_monday}' and '{before_seven}'   ---МЕНЯЕМ ПОНЕДЕЛЬНИК
                              and toDate (updated_at_dt) between  '{before_monday}' and '{before_seven}'	    ---МЕНЯЕМ ПОНЕДЕЛЬНИКИ И ВОСКРЕСЕНЬЯ
                 ---выираем только те лиды, которые созданы в указанный период
                         group by id,pipeline_id,name,custom_fields
                    ) r1
                    left join
                               (
                                select
                                        id as l_id
                                        ,name as name_lead
                                        ,tags
                                  from talenttech.amocrm_tt_leads
                                 ) r2
                    on 	toString(id)=toString(l_id)
            ) ok
            left join talenttech.amocrm_tt_leads lead on toString(ok.id)=toString(lead.id)
            where tags not like '%Лендинг%' or tags  not like '%Заявка с сайта%'  --условие на лиды где нет в тегах Лендинг и Заявка с сайта
            ---делаем закрыто и нереализовано
            )
            --) now ---УБРАТЬ ПОСЛЕ ДЕБАГА
    union all
              (
    ---Верхняя воронка без Заявок с сайта
            --Верхняя воронка без Заявок с сайта
                select
                old_status_id
                ,lead_id
                ,pipeline_id
                ,upd
                ,created_dt
                ,voronka
                ,status_name
                ,name_lead
                ,'' as tags
                , '' as custom_fields
                from
    --ОК
                (
                select
                old_status_id
                ,toInt32OrNull(id) as lead_id
                ,toInt32OrNull(pipeline_id) as pipeline_id
                ,upd
                ,created_dt
                ,toInt32OrNull(arrayJoin(synt_arr_new)) as voronka
                ,indexOf(status_array_2, toString(voronka)) as ind_voronka
                ---,(arrayJoin(status_number)) as voronka_1
                --,status_number
                ,ind_old
                ,ind_new
                from
                (
    --Выбирается максимальный статус по Дате обновления для того чтобы не было дублей кампаний в дальнейшем
    --т.е. если в неделе были несколько изменение то берется последнее изменение и от него строиться воронка
    --т.е. на примере 18274365 id не будут включены переходы в связался с ЛПР, будут только Не обработано
                --смотрим какой по счету был old_status_id в массиве он был третьим
                select
                *
                ,indexOf(status_array, old_status_id) as ind_old
                ,indexOf(status_array, new_status_id) as ind_new
                ,arraySlice(status_array, 1, ind_old) as synt_arr_old
                ,arraySlice(status_array, 1, ind_new) as synt_arr_new
                from
                (
                --Берем id лида из FL
                select  stc.id
                        , led.name
                        ,stc.pipeline_id
                        , stc.old_status_id
                        , argMax (stc.status_id, stc.updated_at_dt) as new_status_id
                        , argMax (stc.created_at_dt, stc.updated_at_dt) as created_dt
                        , max (stc.updated_at_dt) as upd
                 from talenttech.stt_amocrm_lead_status_change_mt  stc left join talenttech.amocrm_tt_leads led on toString(led.id) =stc.id
                where toInt32(stc.status_id)!=143
                  and toDate (stc.created_at_dt) < '{before_monday}'								  ---МЕНЯЕМ ПОНЕДЕЛЬНИК
                  and toDate (stc.updated_at_dt) between  '{before_monday}' and '{before_seven}'      ---МЕНЯЕМ ПОНЕДЕЛЬНИКИ И ВОСКРЕСЕНЬЯ
                  and stc.pipeline_id='2289430'
                  and (tags not like '%Лендинг%' or tags  not like '%Заявка с сайта%')
                 -- and stc.id='20181581'
                group by stc.id,stc.pipeline_id, led.name,stc.old_status_id
                --Берем id лида из FL
                ) l
                left join
                    (
                    --оборачиваем в массив
                    select id
                          ,groupArray(status_id) as status_array
                          ,groupArray(rn) as status_number
                    from
                        --берем все id статусов и сортируем от боьшего к меньшему
                        (
                          select id
                                ,rowNumberInBlock() as rn
                                ,status_id
                            from talenttech.amocrm_tt_funnels_statuses
                           where id=2289430
                        order by toInt32(status_sort)
                        )
                        --берем все id статусов и сортируем от боьшего к меньшему
                    group by id
                    --оборачиваем в массив
                    ) ar
                on l.pipeline_id=toString(ar.id)
                --смотрим какой по счету был old_status_id в массиве он был третьим
                where ind_new > ind_old or (ind_old=12 and ind_new < ind_old)   ----уСЛОВИЕ, ЕСЛИ ЛИД ПЕРЕШЕЛ ИЗ ЗАКРТО И НЕРЕАЛИЗОВАНО В ДРУГОЙ СТАТУС ТО СЧИТАЕМ
                ) g
    --ОК
                left join
                (
                    --оборачиваем в массив
                    select id
                          ,groupArray(status_id) as status_array_2
                          ,groupArray(rn) as status_number
                    from
                        --берем все id статусов и сортируем от боьшего к меньшему
                        (
                          select id
                                ,rowNumberInBlock() as rn
                                ,status_id
                            from talenttech.amocrm_tt_funnels_statuses
                           where id=2289430
                        order by toInt32(status_sort)
                        )
                        --берем все id статусов и сортируем от боьшего к меньшему
                    group by id
                    --оборачив
                )ar2
                on g.pipeline_id=toString(ar2.id)
                where  toInt32(ind_voronka) >toInt32(ind_old) or (toInt32(ind_old)=12 and toInt32(ind_voronka) <toInt32(ind_old))       ----условие исключение старого статуса и создание воронки только по новому статусу
                ) final_table
                left join talenttech.amocrm_tt_funnels_statuses status on voronka= toInt32OrNull(status.status_id)
                left join
                    (
                    select
                            id as l_id
                            ,name as name_lead
                            ,tags
                      from talenttech.amocrm_tt_leads
                     where pipeline_id =2289430
                     --and l_id=18274365
                     ) g2
                    on l_id=lead_id
                 where  status.id =2289430
                ---Верхняя воронка без Заявок с сайта
                )
    union all
    ---делаем закрыто и нереализовано для верхней воронки  ГОТОВО!!
          (
           select  ok.old_status_id as old_status_id
                    ,toInt32OrNull(ok.id) as  lead_id
                    ,toInt32OrNull(ok.pipeline_id)  as pipeline_id
                    ,ok.upd as upd
                    ,ok.created_at_d as created_at_dt
                    ,143 as voronka
                    ,'Закрыто и не реализовано' as status_name
                    ,ok.name  as name_lead
                    ,tags
                    ,custom_fields
            from
            --получилось 38 а не 37 из за 21758379 лида
             (
            select	id
                    ,name_lead as name
                    ,pipeline_id
                    , old_status_id
                    , created_at_d
                    , upd
                    ,custom_fields
              from
                    (
                            select  id
                                    ,name
                                    ,custom_fields
                                    ,pipeline_id
                                    , argMax (status_id, updated_at_dt) as old_status_id
                                    , argMax (created_at_dt, updated_at_dt) as created_at_d
                                    , max (updated_at_dt) as upd
                             from talenttech.stt_amocrm_lead_status_change_mt
                            where toInt32(status_id)=143
                              and pipeline_id='2289430'
                              and toDate (created_at_dt) < '{before_monday}' 							 ---МЕНЯЕМ ПОНЕДЕЛЬНИК
                              and toDate (updated_at_dt) between  '{before_monday}' and '{before_seven}'		 ---МЕНЯЕМ ПОНЕДЕЛЬНИКИ И ВОСКРЕСЕНЬЯ
                 ---выираем только те лиды, которые созданы в указанный период
                         group by id,pipeline_id,name,custom_fields
                    ) r1
                    left join
                               (
                                select
                                        id as l_id
                                        ,name as name_lead
                                        ,tags
                                  from talenttech.amocrm_tt_leads
                                 ) r2
                    on 	toString(id)=toString(l_id)
            ) ok
            left join talenttech.amocrm_tt_leads lead on toString(ok.id)=toString(lead.id)
            where tags not like '%Лендинг%' or tags  not like '%Заявка с сайта%'  --условие на лиды где нет в тегах Лендинг и Заявка с сайта
            ---делаем закрыто и нереализовано для верхней воронки  ГОТОВО!!
            )
         )final_t'''
a= sql.format(before_monday=before_monday,before_seven=before_seven)
print (a)
client.execute(a)

# передавать логин и пароль через переменную окружения
#2) Результат 2-ого этапа - это выполненный запрос за переменную ds, указать в envirement variables
#3) Github