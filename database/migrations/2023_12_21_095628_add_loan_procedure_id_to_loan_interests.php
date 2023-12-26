<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class AddLoanProcedureIdToLoanInterests extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::table('loan_interests', function (Blueprint $table) {
            $table->bigInteger('loan_procedure_id')->nullable();
            $table->foreign('loan_procedure_id')->references('id')->on('loan_procedures');
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        Schema::table('loan_interests', function (Blueprint $table) {
            //
        });
    }
}
