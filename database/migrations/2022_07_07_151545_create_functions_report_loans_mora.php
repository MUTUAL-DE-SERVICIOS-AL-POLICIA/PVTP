<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateFunctionsReportLoansMora extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {   
        
    DB::statement("
    CREATE OR REPLACE FUNCTION public.days_mora(
                id_loan bigint,
                rqst_date date,
                type_mora character varying)
                RETURNS integer
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE PARALLEL UNSAFE
            AS $$
                DECLARE
                days integer;
                BEGIN
                    IF (type_mora = 'mora') 
                    THEN
                        SELECT (diff_in_days((SELECT lpv.estimated_date FROM last_payment_validated(id_loan) lpv)::date, rqst_date::date) - (SELECT lgp.days_current_interest FROM loan_global_parameters lgp)) INTO days;
                        RETURN days;
                    END IF;
                    
                    IF (type_mora = 'mora_parcial')
                    THEN
                        IF (SELECT estimated_date from last_payment_validated(id_loan)) IS NOT NULL
                        THEN
                            SELECT diff_in_days((SELECT lpv.estimated_date FROM last_payment_validated(id_loan) lpv)::date, rqst_date::date) INTO days;
                        ELSE
                            SELECT diff_in_days((SELECT l.disbursement_date FROM loans l WHERE id = id_loan)::date, rqst_date::date) INTO days;
                        END IF;
                        RETURN days;
                    END IF;
                    
                    IF (type_mora = 'mora_total')
                    THEN
                        SELECT (diff_in_days((SELECT l.disbursement_date FROM loans l WHERE l.id = id_loan)::date, rqst_date::date) - (SELECT lgp.days_current_interest FROM loan_global_parameters lgp)) INTO days;
                        RETURN days;
                    END IF;
                END
            $$;
            ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.monthly_current_interest(
                id_loan bigint)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    monthly_interest numeric;
                begin
                    select (li.annual_interest/(100*12))
                    into monthly_interest
                    from loans l, loan_interests li
                    where l.id = id_loan
                        and li.id = l.interest_id;
                    
                    return monthly_interest;
                END
            $$;");

        DB::statement("    
            CREATE OR REPLACE FUNCTION public.total_payment_capital_loan(
                id_loan numeric)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                declare sum_capital_payment numeric;
                begin
                     select sum(lp.capital_payment) into sum_capital_payment
                     from loans l 
                     join loan_payments lp on l.id =lp.loan_id
                     where lp.deleted_at is null
                        and l.id = id_loan
                        and (lp.state_id = 3 or lp.state_id = 4);
                return coalesce(sum_capital_payment,0);
                END;
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.diff_in_days(
                date_1 date,
                date_2 date)
                RETURNS integer
                LANGUAGE 'plpgsql'
            AS $$
                begin
                    return abs(date_1::date - date_2::date);
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.balance_loan(
                id_loan numeric)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                declare balance numeric;
                begin
                     select l.amount_approved - total_payment_capital_loan(l.id)
                     into balance
                     from loans l
                     where l.id = id_loan;
                return round(coalesce(balance,0),2);
                end;
            $$;
        ");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.estimated_quota(
                id_loan bigint)
                RETURNS numeric
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    estimated_quota numeric;
                begin
                    select (monthly_current_interest(id_loan) * l.amount_approved/(1 - 1/pow((1 + monthly_current_interest(id_loan)),l.loan_term)))
                    into estimated_quota
                    from loans l
                    where l.id = id_loan;
                    
                    return round(estimated_quota,2)::numeric;
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.last_payment_validated(
                id_loan bigint)
                RETURNS SETOF loan_payments 
                LANGUAGE 'plpgsql'
            AS $$
                begin
                    return query
                    select *
                    from loan_payments lp
                    where lp.deleted_at is null                      -- NAME: 'Pendiente por confirmar', ID: 3
                        and (lp.state_id = 3 or lp.state_id = 4)     -- NAME: 'Pagado', ID: 4
                        and lp.loan_id = id_loan
                    order by lp.quota_number desc, lp.created_at desc
                    limit 1 offset 0;
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.control_disbursement_date(
                id_loan bigint,
                rqst_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    cond_day integer;
                    cond_month integer;
                    cond_year integer;
                    offint_day integer;
                begin
                    select extract(day from l.disbursement_date), extract(month from l.disbursement_date),
                            extract(year from l.disbursement_date) into cond_day, cond_month, cond_year
                    from loans l
                    where l.id = id_loan and l.deleted_at is null limit 1 offset 0;
                    
                    select lgp.offset_interest_day into offint_day from loan_global_parameters lgp limit 1 offset 0;
                    -- TRUE = Fecha de desembolso despues de un dia 15
                    -- FALSE = Fecha de desembolso antes del dia 15
                    return ((cond_day > offint_day)
                            and ((cond_month = (select extract(month from rqst_date)))
                            or (cond_month = ((select extract(month from rqst_date)) - 1)))
                            and (cond_year = (select extract(year from rqst_date))));
                END
            $$;");
            
        DB::statement("
            CREATE OR REPLACE FUNCTION public.regular_payments_date(
                id_loan bigint,
                rqst_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    date_eval timestamp;
                    qta_number integer;
                    sw boolean;
                    reg record;
                    ttl_amount float8;
                begin
                    date_eval = date_trunc('day', rqst_date) + interval '1 day' - interval '1 second';
                    qta_number = 1;
                    sw = true;
                    
                    for reg in (select * 
                                from loan_payments lp 
                                where lp.loan_id = id_loan 
                                    and lp.estimated_date <= date_eval 
                                    and lp.state_id = 4 /* NAME: 'Pagado', ID: 4 */
                                    and lp.deleted_at is null) loop
                        
                        select lpp.total_amount into ttl_amount
                        from loan_plan_payments lpp
                        where lpp.loan_id = id_loan and lpp.quota_number = qta_number
                            and lpp.deleted_at is null limit 1 offset 0;
                                
                        if(reg.estimated_quota < ttl_amount) then
                            sw = false;
                            exit;
                        end if;
                        qta_number = qta_number + 1;
                    end loop;
                    return sw;
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_mora_parcial(
                id_loan bigint,
                final_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    cond_1 integer;
                    cond_2 boolean;
                begin
                    select count(*) into cond_1 from last_payment_validated(id_loan);
                    select regular_payments_date(id_loan, final_date) into cond_2;
            
                    return ((cond_1 > 0) and (not cond_2));	
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_mora_total(
                id_loan bigint,
                final_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    cond_1 integer;
                    cond_2 integer;
                    cond_3 boolean;
                    days_current_interest integer;
                begin
                    select count(lp.loan_id) into cond_1 from loan_payments lp where lp.deleted_at is null and lp.loan_id = id_loan;
                    select diff_in_days((select lc.disbursement_date from loans lc where lc.id = id_loan)::date,final_date) into cond_2;
                    select lgp.days_current_interest into days_current_interest from loan_global_parameters lgp limit 1 offset 0;
                    select control_disbursement_date(id_loan, final_date) into cond_3;
                    
                    return ((cond_1 = 0) and (cond_2 > days_current_interest) and (not cond_3));	
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loan_mora(
                id_loan bigint,
                final_date date)
                RETURNS boolean
                LANGUAGE 'plpgsql'
            AS $$
                declare
                    cond_1 integer;
                    cond_2 date;
                    cond_3 integer;
                    days_current_interest integer;
                begin
                    select count(lp.loan_id) into cond_1 from loan_payments lp where lp.deleted_at is null and lp.loan_id = id_loan;
                    select estimated_date into cond_2 from last_payment_validated(id_loan);
                    select extract(days from (final_date::timestamp - cond_2::timestamp)) into cond_3;
                    select lgp.days_current_interest into days_current_interest from loan_global_parameters lgp limit 1 offset 0;
                    
                    return ((cond_1 > 0) and (cond_2 < final_date) and (cond_3 > days_current_interest));
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.list_loans_mora(
                final_date date,
                OUT loan_id bigint,
                OUT type_mora character varying)
                RETURNS SETOF record 
                LANGUAGE 'plpgsql'    
            AS $$
                DECLARE
                    reg RECORD;
                BEGIN
                    FOR reg IN select *
                                from loans l
                                where l.state_id = (select ls.id from loan_states ls where ls.name = 'Vigente')
                                and l.disbursement_date < final_date::date LOOP
                        -- MORA
                        if(loan_mora(reg.id,final_date)) then
                            loan_id := reg.id;
                            type_mora := 'mora';
                            RETURN NEXT;
                            continue;
                        end if;
                        -- MORA TOTAL
                        if(loan_mora_total(reg.id,final_date)) then
                            loan_id := reg.id;
                            type_mora := 'mora_total';
                            RETURN NEXT;
                            continue;
                        end if;
                        -- MORA PARCIAL
                        if(loan_mora_parcial(reg.id,final_date)) then
                            loan_id := reg.id;
                            type_mora := 'mora_parcial';
                            RETURN NEXT;
                            continue;
                        end if;
                    END LOOP;
                    RETURN;
                END
            $$;");

        DB::statement("
            CREATE OR REPLACE FUNCTION public.loans_mora_data(
                final_date character varying)
                RETURNS SETOF json 
                LANGUAGE 'plpgsql'    
            AS $$
                begin
                    return query
                    select json_agg(to_json(loans_mora)) from
                    (   select 
                            l.id,
                            llm.type_mora as type,
                            -- AFFILIATE
                            vlb.registration_affiliate, vlb.identity_card_affiliate, vlb.city_exp_first_shortened_affiliate, vlb.full_name_affiliate
                            -- BORROWER
                            ,vlb.registration_borrower, vlb.identity_card_borrower, vlb.city_exp_first_shortened_borrower, vlb.full_name_borrower
                            ,(select to_json(ph) from (select string_to_array(lb.phone_number,',') number) ph) as phone
                            ,(select to_json(cell) from (select string_to_array(lb.cell_phone_number,',') number) cell) as cell_phone
                            --ADDRESS
                            ,(select json_agg(to_json(ad)) from (
                                select ct.name, ad.zone, ad.street, ad.number_address, ad.description
                                from addresses ad, cities ct
                                where lb.address_id = ad.id and ad.city_address_id = ct.id
                            ) ad) as address
                            -- LOAN 
                            ,l.code, l.disbursement_date, l.loan_term
                            ,li.annual_interest, (select lpv.estimated_date from last_payment_validated(l.id) lpv) ,(select pm2.shortened from last_payment_validated(l.id) lpv2, procedure_modalities pm2 where pm2.id = lpv2.procedure_modality_id)
                            ,estimated_quota(l.id),balance_loan(l.id) as balance
                            ,ast.name, pt.second_name, pm.shortened as sub_modality
                            ,days_mora(l.id, final_date::date, llm.type_mora) as days_mora
                            -- PERSONA DE REFERENCIA
                            ,(select json_agg(to_json(r)) from (
                                select concat_full_name(pr.first_name, pr.second_name, pr.last_name, pr.mothers_last_name, pr.surname_husband) as full_name, pr.phone_number, pr.cell_phone_number, pr.address
                                from loan_persons lp, personal_references pr
                                where lp.loan_id = l.id and lp.personal_reference_id = pr.id
                            ) r) as reference
                            -- GARANTES
                            ,(select json_agg(to_json(grt)) from (
                                select vlg.registration_affiliate, vlg.identity_card_affiliate
                                        ,vlg.city_exp_first_shortened_affiliate, vlg.full_name_affiliate
                                        ,vlg.registration_guarantor, vlg.identity_card_guarantor
                                        ,vlg.city_exp_first_shortened_guarantor, vlg.full_name_guarantor
                                        ,lg.phone_number, (select to_json(cellg) from (select string_to_array(lg.cell_phone_number,',') number) cellg) as cell_phone
                                        ,(select ast2.name 
                                          from affiliate_states as2, affiliate_state_types ast2 
                                          where as2.affiliate_state_type_id = ast2.id 
                                            and lg.affiliate_state_id = as2.id)
                                from view_loan_guarantors vlg
                                join loan_guarantors lg on lg.affiliate_id = vlg.id_affiliate
                                where vlg.id_loan = l.id and l.id = lg.loan_id
                            ) grt) as guarantors
            
                        from list_loans_mora(final_date::date) llm
                                , view_loan_borrower vlb, loan_borrowers lb, loans l, loan_interests li
                                , affiliate_states afs, affiliate_state_types ast, procedure_modalities pm, procedure_types pt
                     
                        where   llm.loan_id = l.id
                                and vlb.id_loan = lb.loan_id
                                and l.id = vlb.id_loan
                                and li.id = l.interest_id
                                and	afs.id = lb.affiliate_state_id
                                and ast.id = afs.affiliate_state_type_id
                                and pm.id = l.procedure_modality_id
                                and pm.procedure_type_id = pt.id
                        order by l.code
                   ) loans_mora;
                END
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
            DROP FUNCTION days_mora;");
        DB::statement("
            DROP FUNCTION loans_mora_data;");
        DB::statement("
            DROP FUNCTION list_loans_mora;");
        DB::statement("
            DROP FUNCTION loan_mora;");
        DB::statement("
            DROP FUNCTION loan_mora_total;");
        DB::statement("
            DROP FUNCTION loan_mora_parcial;");
        DB::statement("
            DROP FUNCTION regular_payments_date;");
        DB::statement("
            DROP FUNCTION control_disbursement_date;");
        DB::statement("
            DROP FUNCTION last_payment_validated;");
        DB::statement("
            DROP FUNCTION estimated_quota;");
        DB::statement("
            DROP FUNCTION balance_loan;");
        DB::statement("
            DROP FUNCTION diff_in_days;");
        DB::statement("
            DROP FUNCTION monthly_current_interest;");
        DB::statement("
            DROP FUNCTION total_payment_capital_loan;");
    }
}
