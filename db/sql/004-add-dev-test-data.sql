BEGIN;

DO $$
    DECLARE today TIMESTAMP;
    DECLARE doy INTEGER;
    DECLARE sdate TIMESTAMP;
    DECLARE edate TIMESTAMP;
BEGIN
    today := date_trunc('day', now());
    -- today := date_trunc('day', TIMESTAMP '2024-09-30T00:00:00.000Z');
    -- today := date_trunc('day', TIMESTAMP '2024-10-01T00:00:00.000Z');
    doy := DATE_PART('doy', today);
    sdate := today - make_interval(days => (doy - 92));

    IF (doy < 275) THEN
        sdate := sdate - interval '1 year';
    END IF;

    edate := sdate + INTERVAL '2 years'  - INTERVAL '1 day';

    DROP TABLE IF EXISTS _accrual_targets;
    DROP TABLE IF EXISTS _agreements;
    DROP TABLE IF EXISTS _accruals;

    CREATE TEMPORARY TABLE _accrual_targets AS
        SELECT
            *
        FROM (
            VALUES 
                ('e502eebb-4663-4e5b-9445-9a20441c18d9', 1920 * 60),
                ('5f06e6ce-1422-4a0c-89dd-f4952e735202', 480 * 60)
            )  AS t (accrual_type_id, target_total);


    CREATE TEMPORARY TABLE _agreements AS
        SELECT
            gen_random_uuid() as id,
            '00000000-0000-0000-0000-000000000000' as tenant_id,
            '00000000-0000-0000-0000-000000000000' as person_id,
            generate_series as start_date,
            generate_series + INTERVAL '1 year' - INTERVAL '1 day' as end_date,
            jsonb('{"agreementType": "AHA", "fteValue": 1.0000, "termsAndConditions": "MODERNISED", "salaryBasis": "NATIONAL"}') as contractual_terms
        FROM generate_series(
                sdate,
                edate,
                INTERVAL '1 year'
            );

    INSERT INTO accruals.agreement (id, tenant_id, person_id, start_date, end_date, contractual_terms)
    SELECT id, tenant_id, person_id, start_date, end_date, contractual_terms FROM _agreements;


    INSERT INTO accruals.agreement_target (id, tenant_id, agreement_id, accrual_type_id, target_total)
        SELECT
            gen_random_uuid() as id,
            '00000000-0000-0000-0000-000000000000' as tenant_id,
            _agreements.id,
            _accrual_targets.accrual_type_id,
            _accrual_targets.target_total
        FROM
            _agreements
            CROSS JOIN _accrual_targets;

    CREATE TEMPORARY TABLE _accruals AS
        SELECT
             _a.id as agreement_id,
             dates as accrual_date,
             at.accrual_type_id,
             DIV(at.target_total, 365) as daily_target,
             _a.person_id as person_id
        FROM generate_series(
                sdate,
                edate,
                INTERVAL '1 day'
            ) as dates
            INNER JOIN _agreements AS _a ON
                _a.start_date <= dates
            AND _a.end_date >= dates
            INNER JOIN accruals.agreement_target AS at ON
                at.agreement_id = _a.id::varchar;

    INSERT INTO accruals.accrual (id, tenant_id, agreement_id, accrual_date, accrual_type_id, cumulative_target, cumulative_total, person_id)
    SELECT 
        gen_random_uuid() as id,
        '00000000-0000-0000-0000-000000000000' as tenant_id,
        agreement_id,
        accrual_date,
        accrual_type_id,
        sum(daily_target) OVER (PARTITION BY agreement_id, accrual_type_id ORDER BY accrual_date) as cumulative_target,
        0,
        person_id
    FROM _accruals;


    DROP TABLE IF EXISTS _accrual_targets;
    DROP TABLE IF EXISTS _agreements;
    DROP TABLE IF EXISTS _accruals;

    RAISE NOTICE 'Today: %', today;
    RAISE NOTICE 'Start date: %', sdate;
    RAISE NOTICE 'End date: %', edate;
END $$;

COMMIT;