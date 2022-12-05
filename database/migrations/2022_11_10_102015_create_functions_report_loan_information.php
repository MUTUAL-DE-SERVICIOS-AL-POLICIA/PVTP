<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateFunctionsReportLoanInformation extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        DB::statement("
            CREATE OR REPLACE FUNCTION public.end_of_day(
                rqst_date timestamp)
                RETURNS timestamp
                LANGUAGE 'plpgsql'
            AS $$
                BEGIN
                    RETURN (rqst_date::date + '1 day'::interval)::timestamp - '1 second'::interval;
                END
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.end_of_month(
                rqst_date timestamp)
                RETURNS timestamp
                LANGUAGE 'plpgsql'
            AS $$
                BEGIN
                    RETURN (EXTRACT('year' FROM rqst_date) || '-' ||
                            (EXTRACT('month' FROM rqst_date) + 1) ||
                            '-01')::timestamp - '1 second'::interval;
                END
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.start_of_month(
                rqst_date timestamp without time zone)
                RETURNS timestamp without time zone
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
            AS $$
                BEGIN
                    RETURN (EXTRACT('year' FROM rqst_date) || '-' ||
                            EXTRACT('month' FROM rqst_date) || 
                            '-01')::timestamp;
                END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_information_sheet_later(
                rqst_date timestamp without time zone,
                OUT id_loan integer,
                OUT type_sheet_later character varying)
                RETURNS SETOF record 
                LANGUAGE 'plpgsql'
            AS $$
                DECLARE
                    sub_month integer;
                    anio integer;
                    oid integer;
                    reg RECORD;
                BEGIN
                    -- OBTENIENDO EL MES ANTERIOR Y EL AÑO DE LA FECHA INGRESADA
                    SELECT EXTRACT('month' FROM (rqst_date - '1 month'::interval)) INTO sub_month;
                    SELECT EXTRACT('year' FROM rqst_date) INTO anio;
                    SELECT lgp.offset_interest_day INTO oid from loan_global_parameters lgp OFFSET 0 LIMIT 1;
                    -- RECORRIENDO LOS REGISTROS
                    FOR reg IN  SELECT * 
                                FROM loans l
                                WHERE l.deleted_at IS NULL
                                    AND EXTRACT('day' FROM l.disbursement_date) > oid
                                    AND EXTRACT('month' FROM l.disbursement_date) = sub_month
                                    AND EXTRACT('year' FROM l.disbursement_date) = anio
                                ORDER BY l.disbursement_date
                    LOOP
                        IF EXISTS(SELECT pm.id
                                    FROM procedure_modalities pm
                                    WHERE (pm.name LIKE '%Activo%'OR pm.name LIKE '%Disponibilidad%')
                                        AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_sheet_later := 'comando';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                        IF EXISTS(SELECT pm.id
                                FROM procedure_modalities pm
                                WHERE pm.name LIKE '%SENASIR%'
                                    AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_sheet_later := 'senasir';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                    END LOOP;
                    RETURN;
                END
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_information_sheet_before(
                rqst_date timestamp,
                OUT id_loan bigint,
                OUT type_sheet_before character varying)
                RETURNS SETOF record 
                LANGUAGE 'plpgsql'
            AS $$
                DECLARE
                    mes integer;
                    anio integer;
                    oid integer;
                    reg RECORD;
                BEGIN
                    -- OBTENIENDO EL ME Y EL AÑO DE LA FECHA RQST DATE
                    SELECT EXTRACT('month' FROM rqst_date) INTO mes;
                    SELECT EXTRACT('year' FROM rqst_date) INTO anio;
                    SELECT offset_interest_day INTO oid FROM loan_global_parameters lgp OFFSET 0 LIMIT 1;
                    -- RECORRIENDO LOS REGISTROS
                    FOR reg IN SELECT *
                                FROM loans l
                                WHERE l.deleted_at is NULL
                                    AND EXTRACT('day' FROM l.disbursement_date) <= oid
                                    AND EXTRACT('month' FROM l.disbursement_date::date) = mes
                                    AND EXTRACT('year' FROM l.disbursement_date::date) = anio
                                ORDER BY l.disbursement_date
                    LOOP
                        IF EXISTS(SELECT pm.id
                                    FROM procedure_modalities pm
                                    WHERE (pm.name LIKE '%Activo%'OR pm.name LIKE '%Disponibilidad%')
                                        AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_sheet_before := 'comando';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                        IF EXISTS(SELECT pm.id
                                FROM procedure_modalities pm
                                WHERE pm.name LIKE '%SENASIR%'
                                    AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_sheet_before := 'senasir';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                    END LOOP;
                    RETURN;
                END
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_information_sheet_ancient(
                rqst_date timestamp without time zone,
                OUT id_loan bigint,
                OUT type_ancient character varying)
                RETURNS SETOF record 
                LANGUAGE 'plpgsql'
            AS $$
                DECLARE
                    mes integer;
                    anio integer;
                    date_previous timestamp;
                    date_limit timestamp;
                    reg RECORD;
                BEGIN
                    SELECT end_of_month(rqst_date::timestamp - '1 month'::interval) INTO date_previous;
                    SELECT end_of_day((EXTRACT('YEAR' FROM date_previous) || '-' ||
                                    EXTRACT('MONTH' FROM date_previous) || '-' || '15')::timestamp)
                                    INTO date_limit;
                    FOR reg IN  SELECT *
                                FROM loans l
                                WHERE l.deleted_at IS NULL
                                    AND l.state_id = 3 		-- LOAN STATE => Name: Vigente, ID: 3
                                    AND l.disbursement_date <= date_limit
                                ORDER BY l.disbursement_date
                    LOOP
                        IF EXISTS(SELECT pm.id
                                    FROM procedure_modalities pm
                                    WHERE (pm.name LIKE '%Activo%'OR pm.name LIKE '%Disponibilidad%')
                                        AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_ancient := 'comando';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                        IF EXISTS(SELECT pm.id
                                FROM procedure_modalities pm
                                WHERE pm.name LIKE '%SENASIR%'
                                    AND reg.procedure_modality_id = pm.id)
                        THEN
                            id_loan := reg.id;
                            type_ancient := 'senasir';
                            RETURN NEXT;
                            CONTINUE;
                        END IF;
                    END LOOP;
                    RETURN;
                END
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.annual_interest(
                id_loan bigint)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                BEGIN
                    RETURN (SELECT li.annual_interest FROM loan_interests li, loans l WHERE l.id = id_loan AND li.id = l.interest_id);
                END;
            $$;
        ");
        
        DB::statement("
            CREATE OR REPLACE FUNCTION public.round_half_even(
                val numeric,
                prec integer)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
            DECLARE
                retval numeric;
                difference numeric;
                even boolean;
            BEGIN
                retval := round(val,prec);
                difference := retval-val;
                IF abs(difference)*(10::numeric^prec) = 0.5::numeric THEN
                    even := (retval * (10::numeric^prec)) % 2::numeric = 0::numeric;
                    IF not even THEN
                        retval := round(val-difference,prec);
                    END IF;
                END IF;
                RETURN retval;
            END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.payments_kardex(
                id_loan bigint)
                RETURNS SETOF loan_payments 
                LANGUAGE 'plpgsql'
            AS $$
                BEGIN
                RETURN QUERY
                    SELECT *
                    FROM loan_payments lp
                    WHERE lp.deleted_at IS NULL
                        AND lp.state_id IN (SELECT lps.id
                                            FROM loan_payment_states lps
                                            WHERE lps.name = 'Pagado'
                                                OR lps.name = 'Pendiente por confirmar')
                        AND lp.loan_id = id_loan
                    ORDER BY lp.quota_number ASC;
                END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_plan(
                id_loan bigint)
                RETURNS SETOF loan_plan_payments 
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
                ROWS 1000
            
            AS $$
                BEGIN
                    RETURN QUERY 
                        SELECT *
                        FROM loan_plan_payments lpp
                        WHERE lpp.deleted_at IS NULL
                            AND lpp.loan_id = id_loan
                        ORDER BY lpp.quota_number;
                END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.verify_regular_payments(
                id_loan bigint,
                rqst_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
            AS $$
                DECLARE
                    regular boolean;
                    payments_kardex integer;
                    loan_status integer;
                BEGIN
                    regular := true;
                    payments_kardex := (select count(*)  from payments_kardex(id_loan) pk where pk.estimated_date <= rqst_date::date);
                    loan_status := (select count(*) from (
                                                    select lp.total_amount, lp.estimated_date, lp.quota_number from loan_plan(id_loan) lp where lp.estimated_date <= rqst_date::date
                                                    intersect
                                                    select pk.estimated_quota, pk.estimated_date, pk.quota_number from payments_kardex(id_loan) pk where pk.estimated_date <= rqst_date::date) T);
                    RETURN payments_kardex = loan_status;
                END;
            $$;
        ");
        
        DB::statement("
            CREATE OR REPLACE FUNCTION public.interest_by_days(
                days integer,
                annual_interest numeric,
                balance numeric)
                RETURNS numeric
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
            AS $$
                BEGIN
                RETURN round_half_even(((annual_interest/100)/360)*days*balance,2);
                END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.get_amount_payment(
                id_loan bigint,
                loan_payment_date timestamp without time zone,
                liquidate boolean,
                type character)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                DECLARE
                    quota numeric;
                    penal_interest numeric;
                    suggested_amount numeric;
                    estimated_date timestamp;
                    remaining numeric;
                    days numeric;
                    disbursement_date timestamp;
                    interest_by_days numeric;
                    date_ini numeric;
                    date_pay timestamp;
                    extra_days integer;
                BEGIN
                    quota := 0;
                    penal_interest := 0;
                    suggested_amount := 0;
                    IF liquidate THEN
                        remaining := 0;
                        IF (SELECT NOT EXISTS(SELECT * FROM last_payment_validated(id_loan) lpv WHERE lpv.id = id_loan))THEN
                            SELECT diff_in_days(
                                end_of_day(l.disbursement_date::timestamp)::date,
                                end_of_day(loan_payment_date::timestamp)::date)
                            INTO days
                            FROM loans l
                            WHERE l.id = id_loan
                            OFFSET 0 LIMIT 1;
                        ELSE
                            SELECT diff_in_days(
                                end_of_day(lpv.estimated_date::timestamp)::date,
                                end_of_day(loan_payment_date::timestamp)::date)
                            INTO days
                            FROM last_payment_validated(id_loan) lpv
                            WHERE lpv.id = id_loan
                            OFFSET 0 LIMIT 1;
                            
                            remaining := (SELECT lpv.interest_accumulated + lpv.penal_accumulated FROM last_payment_validated(id_loan) lpv); 
                        END IF;
                        interest_by_days := interest_by_days(days,(SELECT li.annual_interest FROM loan_interests li WHERE li.loan_id = id_loan),balance_loan(id_loan));
                        IF (days > (SELECT lgp.days_current_interest + lgp.grace_period FROM loan_global_parameters OFFSET 0 LIMIT 1)) THEN
                            penal_interest := interest_by_days(days - (SELECT lgp.days_current_interest FROM loan_global_parameters lgp OFFSET 0 LIMIT 1), (SELECT li.penal_interest FROM loan_interests li WHERE li.loan_id = id_loan), balance_loan(id_loan));
                        END IF;
                        suggested_amount := balance_loan(id_loan) + interest_by_days + penal_interest + remaining;
                    ELSE
                        IF (type = 'T') THEN
                            IF NOT EXISTS(SELECT * FROM last_payment_validated(id_loan))
                            THEN
                                date_ini := (SELECT EXTRACT('day' FROM l.disbursement_date) FROM loans l WHERE l.id = id_loan);
                                IF (date_ini <= (SELECT lgp.offset_interest_day FROM loan_global_parameters lgp OFFSET 0 LIMIT 1)) THEN
                                    suggested_amount := estimated_quota(id_loan);
                                ELSE
                                    date_pay := end_of_month(start_of_month((SELECT l.disbursement_date FROM loans l WHERE l.id = id_loan OFFSET 0 LIMIT 1)::timestamp) + '1 month'::interval)::timestamp;
                                    loan_payment_date := (loan_payment_date::date)::timestamp;
                                    IF (loan_payment_date < date_pay)
                                    THEN
                                        extra_days := diff_in_days((SELECT l.disbursement_date FROM loans l WHERE l.id = id_loan)::date, end_of_month((SELECT l.disbursement_date FROM loans l WHERE l.id = id_loan)::timestamp)::date);
                                        suggested_amount := interest_by_days(extra_days,(SELECT li.annual_interest FROM loans l,loan_interests li WHERE li.id = l.interest_id AND l.id = id_loan), balance_loan(id_loan)) + estimated_quota(id_loan);
                                    ELSE
                                        suggested_amount := estimated_quota(id_loan);
                                    END IF;
                                END IF;
                            ELSE
                                IF (verify_regular_payments(id_loan,loan_payment_date::date) AND ((SELECT count(pk.id) FROM payments_kardex(id_loan) pk) + 1) = (SELECT l.loan_term FROM loans l WHERE l.id = id_loan))
                                THEN
                                    days := EXTRACT('day' FROM loan_payment_date::timestamp);
                                    interest_by_days := interest_by_days(days::integer,(SELECT li.annual_interest FROM loan_interests li, loans l WHERE li.id = l.interest_id AND l.id = id_loan),balance_loan(id_loan));
                                    suggested_amount := interest_by_days + balance_loan(id_loan);
                                ELSE
                                    IF (balance_loan(id_loan) > estimated_quota(id_loan))
                                    THEN
                                        suggested_amount := estimated_quota(id_loan);
                                    ELSE
                                        days := diff_in_days((SELECT lpv.estimated_date FROM last_payment_validated(id_loan) lpv),loan_payment_date::date);
                                        interest_by_days := interest_by_days(days::integer,annual_interest(id_loan),balance_loan(id_loan));
                                        suggested_amount := interest_by_days + balance_loan(id_loan);
                                    END IF;
                                END IF;
                            END IF;
                        ELSE
                            suggested_amount := (SELECT lg.quota_treat FROM loan_guarantors lg WHERE lg.loan_id = id_loan OFFSET 0 LIMIT 1);
                        END IF;
                    END IF;
                    RETURN round_half_even(suggested_amount,2);
                END;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_information(
                rqst_date character varying)
                RETURNS TABLE(identity_card_affiliate character varying, fullname_affiliate text, code_loan character varying, disbursement_date_loan timestamp without time zone, identity_card_borrower character varying, last_name_borrower character varying, mothers_last_name_borrower character varying, surname_husband_borrower character varying, first_name_borrower character varying, second_name_borrower character varying, balance_loan numeric, estimated_quota numeric, get_amount_payment numeric, annual_interest numeric, guarantor_amortizing boolean, sheet character varying, type character varying) 
                LANGUAGE 'plpgsql'
            AS $$
            BEGIN
            
            RETURN QUERY
                SELECT 
                    vlb.identity_card_affiliate,							-- CI AFILIADO
                    vlb.full_name_affiliate,								-- NOMBRE COMPLETO
                    vlb.code_loan,											-- COD PRESTAMO
                    vlb.disbursement_date_loan,								-- FECHA DE DESEMBOLSO
                    vlb.identity_card_borrower,								-- CI PRESTATARIO
                    vlb.last_name_borrower,									-- APELLIDO PATERNO
                    vlb.mothers_last_name_borrower,							-- APELLIDO MATERNO
                    vlb.surname_husband_borrower,							-- APELLIDO CASADA
                    vlb.first_name_borrower,								-- PRIMER NOMBRE
                    vlb.second_name_borrower,								-- SEGUNDO NOMBRE
                    balance_loan(vlb.id_loan),								-- SALDO ACTUAL
                    estimated_quota(vlb.id_loan),							-- CUOTA FIJA ACTUAL
                    get_amount_payment(vlb.id_loan,
                        end_of_month(rqst_date::timestamp),false,'T'),		-- DESCUENTO PROGRAMADO
                    annual_interest(vlb.id_loan),							-- INTERES
                    l.guarantor_amortizing,									-- Amort. TIT o GAR?
                    cs.sheet,												-- HOJA
                    cs.type													-- TYPE DE HOJA
                    
            FROM (
                select id_loan, type_sheet_before as type, 'before'::varchar as sheet from loan_information_sheet_before(rqst_date::timestamp) csb
                union all
                select id_loan, type_sheet_later as type, 'later'::varchar as sheet from loan_information_sheet_later(rqst_date::timestamp) csl
                union all
                select id_loan, type_ancient as type, 'ancient'::varchar as sheet from loan_information_sheet_ancient(rqst_date::timestamp) csa
            ) cs
            
            LEFT JOIN view_loan_borrower vlb ON cs.id_loan = vlb.id_loan
            LEFT JOIN loans l ON l.id = cs.id_loan
            ORDER BY cs.id_loan;
            
            END;
            $$;
        ");
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        DB::statement("
            DROP FUNCTION end_of_day;");
        DB::statement("
            DROP FUNCTION end_of_month;");
        DB::statement("
            DROP FUNCTION start_of_month;");
        DB::statement("
            DROP FUNCTION loan_information_sheet_later;");
        DB::statement("
            DROP FUNCTION loan_information_sheet_before;");
        DB::statement("
            DROP FUNCTION loan_information_sheet_ancient;");
        DB::statement("
            DROP FUNCTION annual_interest;");
        DB::statement("
            DROP FUNCTION round_half_even;");
        DB::statement("
            DROP FUNCTION payments_kardex;");
        DB::statement("
            DROP FUNCTION loan_plan;");
        DB::statement("
            DROP FUNCTION interest_by_days;");
        DB::statement("
            DROP FUNCTION verify_regular_payments;");
        DB::statement("
            DROP FUNCTION get_amount_payment;");
        DB::statement("
            DROP FUNCTION loan_information;");
    }
}
