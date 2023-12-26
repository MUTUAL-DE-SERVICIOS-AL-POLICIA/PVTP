<?php

use Illuminate\Database\Seeder;
use App\LoanProcedure;

class LoanProcedureSeeder extends Seeder
{
    /**
     * Run the database seeds.
     *
     * @return void
     */
    public function run()
    {
        $loan_porcedures = [
            ['description'=>'Reglamento de Préstamos 2019','start_production_date'=>'2021-06-04','is_enable'=>false],
            ['description'=>'Reglamento de Préstamos 2022','start_production_date'=>'2023-01-16','is_enable'=>true],
            ['description'=>'Reglamento de Préstamos 2023','start_production_date'=>'2024-01-02','is_enable'=>true],
            ];
            foreach ($loan_porcedures as $loan_porcedure) {
                LoanProcedure::firstOrCreate($loan_porcedure);
            }
    }
}
