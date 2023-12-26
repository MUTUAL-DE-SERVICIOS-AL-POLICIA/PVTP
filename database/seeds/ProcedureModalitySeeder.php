<?php

use Illuminate\Database\Seeder;

use App\ProcedureModality;
use App\LoanInterest;

class ProcedureModalitySeeder extends Seeder

{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        $procedure_modalities = [
        //Creacion de Nuevas Modalidades
        ['procedure_type_id'=>12,'name'=>'Largo Plazo con Garantía Personal en Comisión','shortened'=>'LAR-COM','is_valid'=>true ],
        ['procedure_type_id'=>12,'name'=>'Largo Plazo con Garantía Personal en Disponibilidad','shortened'=>'LAR-DIS','is_valid'=>true ],
        ['procedure_type_id'=>11,'name'=>'Refinanciamiento de Préstamo a Corto Plazo en Disponibilidad','shortened'=>'REF-COR-DIS','is_valid'=>true ],
        //REGGLAMENTO 2023
        ['procedure_type_id'=>12,'name'=>'Largo Plazo con Garantía Personal con un Solo Garante Sector Activo','shortened'=>'LAR-1G','is_valid'=>true],
        ['procedure_type_id'=>12,'name'=>'Largo Plazo con Garantía Personal con dos Garantes Sector Activo','shortened'=>'LAR-2G','is_valid'=>true],
        ['procedure_type_id'=>12,'name'=>'Largo Plazo PPO con un Solo Garante Sector Activo','shortened'=>'LAR-PPO-1G','is_valid'=>true],
        ['procedure_type_id'=>12,'name'=>'Largo Plazo PPO con dos Garantes Sector Activo','shortened'=>'LAR-PPO-2G','is_valid'=>true],
        ['procedure_type_id'=>13,'name'=>'Refinanciamiento de Préstamo a Largo Plazo con un Solo Garante Sector Activo','shortened'=>'REF-LAR-1G','is_valid'=>true],
        ['procedure_type_id'=>13,'name'=>'Refinanciamiento de Préstamo a Largo Plazo con dos Garantes Sector Activo','shortened'=>'REF-LAR-2G','is_valid'=>true],
        ['procedure_type_id'=>13,'name'=>'Refinanciamiento de Préstamo a Largo Plazo PPO con un Solo Garante Sector Activo','shortened'=>'REF-LAR-PPO-1G','is_valid'=>true],
        ['procedure_type_id'=>13,'name'=>'Refinanciamiento de Préstamo a Largo Plazo PPO con dos Garantes Sector Activo','shortened'=>'REF-LAR-PPO-2G','is_valid'=>true],
        
        ];
        foreach ($procedure_modalities as $procedure_modality) {
            ProcedureModality::firstOrCreate($procedure_modality);
        }
        //Actualizacion de Miodalidades
        $procedureModality = ProcedureModality::where('name','Largo Plazo con un Solo Garante Sector Activo CPOP')->first();
        if(isset($procedureModality)){
        $procedureModality->name ="Largo Plazo con un Solo Garante Sector Activo";
        $procedureModality->shortened ="LAR-1G";
        $procedureModality->update();
        }
        $procedureModalityRef = ProcedureModality::where('name','Refinanciamiento de Préstamo a largo Plazo con un Solo Garante Sector Activo CPOP')->first();
        if(isset($procedureModalityRef)){
        $procedureModalityRef->name ="Refinanciamiento de Préstamo a Largo Plazo con un Solo Garante Sector Activo";
        $procedureModalityRef->shortened ="REF-LAR-1G";
        $procedureModalityRef->update();
        }
        //Actualizacion de Parametros tabla LoanInterest de las nuevas modalidades
        $loan_interests = [
            //Creacion de parametros
            ['procedure_modality_id'=>ProcedureModality::where('name','Largo Plazo con Garantía Personal en Comisión')->first()->id,
             'annual_interest'=>13.20,
             'penal_interest'=>6
            ],[
             'procedure_modality_id'=>ProcedureModality::where('name','Largo Plazo con Garantía Personal en Disponibilidad')->first()->id,
             'annual_interest'=>13.20,
             'penal_interest'=>6
            ],[
             'procedure_modality_id'=>ProcedureModality::where('name','Refinanciamiento de Préstamo a Corto Plazo en Disponibilidad')->first()->id,
             'annual_interest'=>20,
             'penal_interest'=>6
            ],
            ];
            foreach ($loan_interests as $loan_interest){
                LoanInterest::firstOrCreate($loan_interest);
            }
    }
}
