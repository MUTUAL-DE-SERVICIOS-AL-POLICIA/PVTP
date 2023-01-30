<template>
  <v-container fluid class="mt-0">
    <v-row>
      <v-col cols="12" class="pb-0 mb-0">
        <p style="color: teal">
          <b>DATOS DE BOLETA</b>
        </p>
      </v-col>
    </v-row>
    <v-row>
      <v-col cols="12" sm="12" :md="col_size" class="mt-0 pt-0">
        <v-data-table
          dense
          :headers="_headers"
          :items="ballot_adjusts"
          hide-default-footer
          class="elevation-1"
        >
          <!-- Boletas de pago y bonos aplicando el formato de moneda -->
          <template v-slot:item="{ item }">
            <tr>
              <td>{{ item.month_year|dateperiod }}</td>
              <td>{{ item.payable_liquid | money }}</td>
              <td>{{ item.mount_adjust | money }}</td>
              <td v-show="type == 'contributions'">{{ item.border_bonus | money }}</td>
              <td v-show="type == 'contributions'">{{ item.position_bonus | money }}</td>
              <td v-show="type == 'contributions'">{{ item.east_bonus | money }}</td>
              <td v-show="type == 'contributions'">{{ item.public_security_bonus | money }}</td>
              <td v-show="type == 'aid_contributions'">{{ item.dignity_rent | money }}</td>
            </tr>
          </template>
          <!-- Promedio de boletas de pago y bonos -->
          <template slot="body.append">
            <tr>
              <td><b>Total Promedio</b></td>
              <td><b>{{ average_ballot_adjust.average_payable_liquid | money }}</b></td>
              <td><b>{{ average_ballot_adjust.average_mount_adjust | money }}</b></td>
              <td v-show="type == 'contributions'"><b>{{ average_ballot_adjust.average_border_bonus | money }}</b></td>
              <td v-show="type == 'contributions'"><b>{{ average_ballot_adjust.average_position_bonus | money }}</b></td>
              <td v-show="type == 'contributions'"><b>{{ average_ballot_adjust.average_east_bonus | money }}</b></td>
              <td v-show="type == 'contributions'"><b>{{average_ballot_adjust.average_public_security_bonus | money}}</b></td>
              <td v-show="type == 'aid_contributions'"><b>{{ average_ballot_adjust.average_dignity_rent | money }}</b></td>
            </tr>
          </template>
        </v-data-table>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
export default {
  name: "workflow-ballots-adjust",
  props: {
    ballots: {
      type: Object,
      required: true,
    },
  },
  data: () => ({
    col_size: 10,
    type: "",
    ballot_adjust: [],
    average_ballot_adjust: null,
    headers: [
      {
        text: "Periodo",
        align: "start",
        sortable: false,
        value: "month_year",
        show: ["con", "aid_con", "loan_con_adj"],
        class: "white--text",
      },
      {
        text: "Liquido",
        sortable: false,
        value: "payable_liquid",
        show: ["con", "aid_con", "loan_con_adj"],
        class: "white--text",
      },
      {
        text: "Monto de ajuste",
        sortable: false,
        value: "mount_adjust",
        show: ["con", "aid_con", "loan_con_adj"],
        class: "white--text",
      },
      {
        text: "Bono Frontera",
        sortable: false,
        value: "border_bonus",
        show: ["con"],
        class: "white--text",
      },
      {
        text: "Bono Cargo",
        sortable: false,
        value: "position_bonus",
        show: ["con"],
        class: "white--text",
      },
      {
        text: "Bono Oriente",
        sortable: false,
        value: "east_bonus",
        show: ["con"],
        class: "white--text",
      },
      {
        text: "Bono Seguridad",
        sortable: false,
        value: "public_security_bonus",
        show: ["con"],
        class: "white--text",
      },
      {
        text: "Bono Renta Dignidad",
        sortable: false,
        value: "dignity_rent",
        show: ["aid_con"],
        class: "white--text",
      },
    ],
  }),
  beforeMount() {
    this.type = this.ballots.contribution_type
    this.ballot_adjusts = this.ballots.ballot_adjusts
    this.average_ballot_adjust = this.ballots.average_ballot_adjust[0]
  },
  computed: {
    //Filtrando los encabezados segun el tipo de contribucion 
    _headers() {
      //tipo aid_contributions para pasivos
      if (this.type == "aid_contributions") {
        this.col_size = "6"
        return this.headers.filter((x) => x.show.includes("aid_con"))
      }
      //tipo loan_contribution_adjusts para comision
      if (this.type == "loan_contribution_adjusts") {
        this.col_size = "4"
        return this.headers.filter(
          (x) => x.show.includes("loan_con_adj") 
        );
      }
      //tipo:contributions para activos
      return this.headers.filter((x) => x.show.includes("con"))
    },
  },
};
</script>