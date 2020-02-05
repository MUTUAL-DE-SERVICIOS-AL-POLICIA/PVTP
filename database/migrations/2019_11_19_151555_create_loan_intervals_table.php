<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class CreateLoanIntervalsTable extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::create('loan_intervals', function (Blueprint $table) {
            $table->bigIncrements('id');
            $table->integer('maximum_amount')->nullable();//monto maximo
            $table->integer('minimum_amount')->nullable();//monto minimo
            $table->integer('maximum_term')->nullable();//plazo maximo en meses 
            $table->integer('minimum_term')->nullable();//plazo minimo en meses
            $table->unsignedBigInteger('procedure_type_id');  // id lugar de la solicitud 
            $table->foreign('procedure_type_id')->references('id')->on('procedure_types');
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        Schema::dropIfExists('loan_intervals');
    }
}
