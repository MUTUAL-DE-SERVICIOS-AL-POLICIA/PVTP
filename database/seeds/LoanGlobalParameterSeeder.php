<?php

use Illuminate\Database\Seeder;
use App\LoanGlobalParameter;

class LoanGlobalParameterSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        $global_parameters = [
         /*   //REGLAMENTO 2021
            ['offset_ballot_day' => 7,
            'offset_interest_day' => 15,
            'livelihood_amount' => 510,
            'min_service_years' =>1,
            'min_service_years_adm' =>2,
            'max_guarantor_active' =>3,
            'max_guarantor_passive' =>2,
            'date_delete_payment'=>1,
            'max_loans_active' => 2,
            'max_loans_process' => 1,
            'days_current_interest' => 31,
            'grace_period' => 3,
            'consecutive_manual_payment' => 3,
            'max_months_go_back' => 10,
            'min_percentage_paid' => 25,
            'min_remaining_installments' => 3,
            'min_amount_fund_rotary' =>10000
        ],
        [   //REGLAMENTO 2022
            'offset_ballot_day' => 7,
            'offset_interest_day' => 15,
            'livelihood_amount' => 0,
            'min_service_years' =>0,
            'min_service_years_adm' =>0,
            'max_guarantor_active' =>3,
            'max_guarantor_passive' =>2,
            'date_delete_payment'=>1,
            'max_loans_active' => 2,
            'max_loans_process' => 1,
            'days_current_interest' => 31,
            'grace_period' => 3,
            'consecutive_manual_payment' => 3,
            'max_months_go_back' => 3,
            'min_percentage_paid' => 25,
            'min_remaining_installments' => 3,
            'min_amount_fund_rotary' => 100000,
            'days_year_calculated'=> 1,
            'loan_procedure_id' => 2,
        ],*/
        [   //REGLAMENTO 2023
            'offset_ballot_day' => 7,
            'offset_interest_day' => 15,
            'livelihood_amount' => 0,
            'min_service_years' =>0,
            'min_service_years_adm' =>0,
            'max_guarantor_active' =>2,
            'max_guarantor_passive' =>1,
            'date_delete_payment'=>1,
            'max_loans_active' => 2,
            'max_loans_process' => 1,
            'days_current_interest' => 31,
            'grace_period' => 3,
            'consecutive_manual_payment' => 3,
            'max_months_go_back' => 3,
            'min_percentage_paid' => 25,
            'min_remaining_installments' => 3,
            'min_amount_fund_rotary' => 100000,
            'loan_procedure_id' => 3,
            'days_year_calculated'=> 1,
            'days_for_import' => 20,
            'numerator' => 360,
            'denominator' => 360
            ]
        ];
        foreach ($global_parameters as $global_parameter) {
            LoanGlobalParameter::firstOrCreate($global_parameter);
        }
    }
}
