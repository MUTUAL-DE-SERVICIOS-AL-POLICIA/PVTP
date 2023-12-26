<?php

use Illuminate\Database\Seeder;
use App\ProcedureModality;
use App\LoanInterest;
use App\LoanModalityParameter;

class ProcedureModalityParameterInteresSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        $procedure_modality_all = ProcedureModality::whereIn('procedure_type_id', [9, 10, 11, 12, 13])->get();
        $procedure_parameters = [
            [
                'loan_modality_parameters' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Activo')->first()->id,
                    'debt_index' => 80,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 5000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 3,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => true,
                    'print_form_qualification_platform' => true,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => false
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Activo')->first()->id,
                    'annual_interest' => 36,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo en Disponibilidad')->first()->id,
                    'debt_index' => 80,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 5000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 3,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => true,
                    'print_form_qualification_platform' => true,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => false
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo en Disponibilidad')->first()->id,
                    'annual_interest' => 36,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Pasivo Gestora Pública')->first()->id,
                    'debt_index' => 50,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 5000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 3,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => true,
                    'print_form_qualification_platform' => true,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => false
                    ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Pasivo Gestora Pública')->first()->id,
                    'annual_interest' => 36,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Pasivo SENASIR')->first()->id,
                'debt_index' => 50,
                'quantity_ballots' => 1,
                'guarantors' => 0,
                'max_lenders' => 1,
                //'min_guarantor_category'=> 0,
                //'max_guarantor_category'=> 0,
                'max_cosigner' => 0,
                'personal_reference' => true,
                'minimum_amount_modality' => 1,
                'maximum_amount_modality' => 5000,
                'minimum_term_modality' => 1,
                'maximum_term_modality' => 3,
                'print_contract_platform' => true,
                'print_receipt_fund_rotary' => true,
                'print_form_qualification_platform' => true,
                'loan_procedure_id' => 3,
                //'max_approved_amount'=> 80000,
                //'guarantor_debt_index'=> 50,
                'avc' => false
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Anticipo Sector Pasivo SENASIR')->first()->id,
                    'annual_interest' => 36,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Activo')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo en Disponibilidad')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo en Disponibilidad')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Pasivo Gestora Pública')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 12,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Pasivo Gestora Pública')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Pasivo SENASIR')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 12,
                    'print_contract_platform' => true,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Corto Plazo Sector Pasivo SENASIR')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo Sector Activo')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo sector Pasivo Gestora Pública')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 12,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo sector Pasivo Gestora Pública')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo Sector Pasivo SENASIR')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 0,
                    'max_lenders' => 1,
                    //'min_guarantor_category'=> 0,
                    //'max_guarantor_category'=> 0,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 25000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 12,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    //'max_approved_amount'=> 80000,
                    //'guarantor_debt_index'=> 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Corto Plazo Sector Pasivo SENASIR')->first()->id,
                    'annual_interest' => 22,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal con un Solo Garante Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal con un Solo Garante Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal con dos Garantes Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal con dos Garantes Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal Sector Pasivo Gestora Pública')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true                    
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal Sector Pasivo Gestora Pública')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal Sector Pasivo SENASIR')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 24,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo con Garantía Personal Sector Pasivo SENASIR')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo PPO con un Solo Garante Sector Activo')->first()->id,
                    'debt_index' => 70,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo PPO con un Solo Garante Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo PPO con dos Garantes Sector Activo')->first()->id,
                    'debt_index' => 70,
                    'quantity_ballots' => 1,
                    'guarantors' => 2,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Largo Plazo PPO con dos Garantes Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo con un Solo Garante Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo con un Solo Garante Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo con dos Garantes Sector Activo')->first()->id,
                    'debt_index' => 60,
                    'quantity_ballots' => 1,
                    'guarantors' => 2,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo con dos Garantes Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo PPO con un Solo Garante Sector Activo')->first()->id,
                    'debt_index' => 70,
                    'quantity_ballots' => 1,
                    'guarantors' => 1,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo PPO con un Solo Garante Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ], [
                'loan_modality_parameters' => [ 
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo PPO con dos Garantes Sector Activo')->first()->id,
                    'debt_index' => 70,
                    'quantity_ballots' => 1,
                    'guarantors' => 2,
                    'max_lenders' => 1,
                    'min_guarantor_category' => 0.35,
                    'max_guarantor_category' => 1,
                    'max_cosigner' => 0,
                    'personal_reference' => true,
                    'minimum_amount_modality' => 1,
                    'maximum_amount_modality' => 300000,
                    'minimum_term_modality' => 1,
                    'maximum_term_modality' => 60,
                    'print_contract_platform' => false,
                    'print_receipt_fund_rotary' => false,
                    'print_form_qualification_platform' => false,
                    'loan_procedure_id' => 3,
                    'max_approved_amount' => 80000,
                    'guarantor_debt_index' => 50,
                    'avc' => true
                ],
                'interest' => [
                    'procedure_modality_id' => $procedure_modality_all->where('name', 'Refinanciamiento de Préstamo a Largo Plazo PPO con dos Garantes Sector Activo')->first()->id,
                    'annual_interest' => 16,
                    'penal_interest' => 6,
                    'loan_procedure_id' => 3
                ]
            ]
        ];

        foreach ($procedure_parameters as $modality_paramet) {
            LoanModalityParameter::firstOrCreate($modality_paramet['loan_modality_parameters']);
            LoanInterest::firstOrCreate($modality_paramet['interest']);
        }
    }
}
