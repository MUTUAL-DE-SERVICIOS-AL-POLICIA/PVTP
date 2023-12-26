<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

class AddAvcColumnLoanModalityParameters extends Migration
{
    /**
     * Run the migrations.
     *
     * @return void
     */
    public function up()
    {
        Schema::table('loan_modality_parameters', function (Blueprint $table) {
            $table->boolean('avc')->default(false)->comment('Campos para habilitar la impresión de la sección del avc dentro el formulario de solictud');
        });
    }

    /**
     * Reverse the migrations.
     *
     * @return void
     */
    public function down()
    {
        Schema::dropIfExists('loan_modality_parameters');
    }
}
