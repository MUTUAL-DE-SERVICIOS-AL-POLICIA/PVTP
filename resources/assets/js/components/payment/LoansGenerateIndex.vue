<template>
  <v-container fluid>
    <ValidationObserver>
      <v-form>
        <v-card flat>
          <v-card-title class="pa-0 pb-3">
            <v-toolbar dense color="tertiary" class="font-weight-regular">
              <v-toolbar-title>PRÉSTAMOS DESEMBOLSADOS</v-toolbar-title>
            </v-toolbar>
          </v-card-title>
          <template>
            <v-container fluid class="pa-0">
              <v-row justify="center" class="py-0">
                <v-col cols="12" class="py-0">
                  <!-- Parece que acá, esta renderizando las acciones -->
                  <v-tabs dark active-class="secondary" v-model="tab">
                    <v-tab v-for="item in actions" :key="item.nameTab">{{
                      item.nameTab
                    }}</v-tab>
                  </v-tabs>
                  <v-tabs-items v-model="tab">
                    <v-tab-item v-for="item in actions" :key="item.nameTab">
                      <v-card flat tile>
                        <v-card-text class="pa-0">
                          <v-row align="center" no-gutters>
                            <v-col cols="12" class="pa-0">
                              <v-layout row wrap>
                                <v-col cols="12" md="12" class="py-2 px-1">
                                  <v-tooltip top>
                                    <template v-slot:activator="{ on }">
                                      <v-btn
                                        fab
                                        @click="download_loans()"
                                        color="success"
                                        v-on="on"
                                        x-small
                                        absolute
                                        right
                                        style="
                                          margin-right: 40px;
                                          margin-top: -50px;
                                        "
                                        :loading="loading_download"
                                      >
                                        <v-icon> mdi-file-excel </v-icon>
                                      </v-btn>
                                    </template>
                                    <span class="caption"
                                      >Descargar reporte</span
                                    >
                                  </v-tooltip>
                                  <v-tooltip top>
                                    <template v-slot:activator="{ on }">
                                      <v-btn
                                        fab
                                        @click="clearAll()"
                                        color="info"
                                        v-on="on"
                                        x-small
                                        absolute
                                        right
                                        style="
                                          margin-right: 0px;
                                          margin-top: -50px;
                                        "
                                      >
                                        <v-icon> mdi-broom </v-icon>
                                      </v-btn>
                                    </template>
                                    <span class="caption"
                                      >Limpiar todos los filtros</span
                                    >
                                  </v-tooltip>
                                  <v-data-table
                                    dense
                                    :headers="headers"
                                    :items="loans"
                                    :options.sync="options"
                                    :server-items-length="totalLoans"
                                    :footer-props="{
                                      itemsPerPageOptions: [8, 15, 30],
                                    }"
                                    :loading="loading_table"
                                  >
                                    <template
                                      v-slot:[`header.code_loan`]="{ header }"
                                    >
                                      <span
                                        :class="
                                          searching.code_loan
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`header.identity_card_borrower`]="{
                                        header,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.identity_card_borrower
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`header.registration_borrower`]="{
                                        header,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.registration_borrower
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`header.full_name_borrower`]="{
                                        header,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.full_name_borrower
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`header.shortened_sub_modality_loan`]="{
                                        header,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.shortened_sub_modality_loan
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`header.state_type_affiliate`]="{
                                        header,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.state_type_affiliate
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{ header.text }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`item.guarantor_amortizing_loan`]="{
                                        item,
                                      }"
                                    >
                                      <span
                                        :class="
                                          searching.guarantor_amortizing_loan
                                            ? 'primary--text'
                                            : ''
                                        "
                                      >
                                        {{
                                          item.guarantor_amortizing_loan == true
                                            ? "T"
                                            : "G"
                                        }}
                                      </span>
                                    </template>
                                    <template
                                      v-slot:[`item.disbursement_date_loan`]="{
                                        item,
                                      }"
                                    >
                                      {{
                                        item.disbursement_date_loan
                                          | datetimeshorted
                                      }}
                                    </template>

                                    <!--  -->
                                    <template
                                      v-slot:[`item.amount_approved_loan`]="{
                                        item,
                                      }"
                                    >
                                      {{ item.amount_approved_loan | money }}
                                    </template>

                                    <!--  -->
                                    <template
                                      v-slot:[`item.balance_loan`]="{ item }"
                                    >
                                      {{ item.balance_loan | money }}
                                    </template>

                                    <!-- Cuota -->
                                    <template
                                      v-slot:[`item.quota_loan`]="{ item }"
                                    >
                                      {{ item.quota_loan | moneyString }}
                                    </template>
                                    <template
                                      v-slot:[`item.actions`]="{ item }"
                                    >
                                      <v-tooltip bottom>
                                        <template v-slot:activator="{ on }">
                                          <v-btn
                                            icon
                                            small
                                            v-on="on"
                                            color="teal"
                                            :to="{
                                              name: 'flowAdd',
                                              params: { id: item.id_loan },
                                              query: { redirectTab: 7 },
                                            }"
                                            ><v-icon
                                              >mdi-text-box-multiple</v-icon
                                            >
                                          </v-btn>
                                        </template>
                                        <span>Kardex</span>
                                      </v-tooltip>
                                      <v-menu
                                        offset-x
                                        close-on-content-click
                                        v-if="
                                          permissionSimpleSelected.includes(
                                            'print-contract-loan'
                                          ) ||
                                          (permissionSimpleSelected.includes(
                                            'print-payment-plan'
                                          ) &&
                                            item.state_loan == 'Vigente') ||
                                          (permissionSimpleSelected.includes(
                                            'print-payment-kardex-loan'
                                          ) &&
                                            item.state_loan == 'Vigente') ||
                                          (permissionSimpleSelected.includes(
                                            'print-payment-plan'
                                          ) &&
                                            item.state_loan == 'Liquidado') ||
                                          (permissionSimpleSelected.includes(
                                            'print-payment-kardex-loan'
                                          ) &&
                                            item.state_loan == 'Liquidado')
                                        "
                                      >
                                        <template v-slot:activator="{ on }">
                                          <v-btn
                                            icon
                                            color="primary"
                                            dark
                                            v-on="on"
                                          >
                                            <v-icon>mdi-printer</v-icon>
                                          </v-btn>
                                        </template>
                                        <v-list dense class="py-0">
                                          <v-list-item
                                            v-for="doc in printDocs"
                                            :key="doc.id"
                                            @click="
                                              imprimir(doc.id, item.id_loan)
                                            "
                                          >
                                            <v-list-item-icon
                                              class="ma-0 py-0 pt-2"
                                            >
                                              <v-icon
                                                class="ma-0 py-0"
                                                small
                                                v-text="doc.icon"
                                                color="light-blue accent-4"
                                              ></v-icon>
                                            </v-list-item-icon>
                                            <v-list-item-title
                                              class="ma-0 py-0 mt-n2"
                                            >
                                              {{ doc.title }}
                                            </v-list-item-title>
                                          </v-list-item>
                                        </v-list>
                                      </v-menu>
                                    </template>

                                    <template slot="body.prepend">
                                      <tr v-if="show_filter">
                                        <td>
                                          <v-text-field
                                            placeholder="Cod Prestamo"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="searching.code_loan"
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Ci. Prestatario"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.identity_card_borrower
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Matricula Prestatario"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.registration_borrower
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Nombre Completo"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.full_name_borrower
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Corto sub modalidad"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.shortened_sub_modality_loan
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            disabled
                                            class="filter-text"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            disabled
                                            class="filter-text"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            disabled
                                            class="filter-text"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            disabled
                                            class="filter-text"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Sector"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.state_type_affiliate
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            placeholder="Amortizar por"
                                            spellcheck="false"
                                            class="filter-text"
                                            v-model="
                                              searching.guarantor_amortizing_loan
                                            "
                                            @keydown.enter="search_loans()"
                                          ></v-text-field>
                                        </td>
                                        <td>
                                          <v-text-field
                                            disabled
                                            class="filter-text"
                                          ></v-text-field>
                                        </td>
                                      </tr>
                                    </template>
                                  </v-data-table>
                                </v-col>
                              </v-layout>
                            </v-col>
                          </v-row>
                        </v-card-text>
                      </v-card>
                    </v-tab-item>
                  </v-tabs-items>
                </v-col>
              </v-row>
            </v-container>
          </template>
        </v-card>
      </v-form>
    </ValidationObserver>
  </v-container>
</template>

<script>
export default {
  name: "payment-loanGenerateList",

  data: () => ({
    tab: 0,
    actions: [
      { nameTab: "Préstamos vigentes", value: "0" },
      { nameTab: "Prestamos cancelados", value: "1" },
    ],
    searching: {
      code_loan: "",
      identity_card_borrower: "",
      registration_borrower: "",
      full_name_borrower: "",
      shortened_sub_modality_loan: "",
      state_type_affiliate: "",
      guarantor_amortizing_loan: "",
    },
    headers: [
      {
        text: "Cód. Préstamo",
        value: "code_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "15%",
      },
      {
        text: "CI Prestatario",
        value: "identity_card_borrower",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Matrícula Prestatario",
        value: "registration_borrower",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Nombre Completo Prestatario",
        value: "full_name_borrower",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "20%",
      },
      {
        text: "Corto Sub modalidad",
        value: "shortened_sub_modality_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "10%",
      },
      {
        text: "Fecha Desembolso",
        value: "disbursement_date_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Monto Desembolsado",
        value: "amount_approved_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Saldo Capital",
        value: "balance_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Cuota",
        value: "quota_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Sector",
        value: "state_type_affiliate",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Amortizar por",
        value: "guarantor_amortizing_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Estado",
        value: "state_loan",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        width: "5%",
      },
      {
        text: "Acción",
        value: "actions",
        input: "",
        menu: false,
        type: "text",
        class: ["normal", "white--text", "text-md-center"],
        sortable: false,
        width: "10%",
      },
    ],
    loans: [],
    printDocs: [],
    options: {
      page: 1,
      itemsPerPage: 8,
      sortBy: ["code_loan"],
      sortDesc: [false],
    },
    totalLoans: 0,
    loading_download: false,
    loading_table: false,
    show_filter: true,
    items: [
      {
        name: "G",
        value: "TRUE",
      },
      {
        name: "T",
        value: "FALSE",
      },
      {
        name: "TODOS",
        value: "",
      },
    ],
  }),

  computed: {
    //permisos del selector global por rol
    permissionSimpleSelected() {
      return this.$store.getters.permissionSimpleSelected;
    },
  },

  watch: {
    options: function (newVal, oldVal) {
      if (
        newVal.page != oldVal.page ||
        newVal.itemsPerPage != oldVal.itemsPerPage ||
        newVal.sortBy != oldVal.sortBy ||
        newVal.sortDesc != oldVal.sortDesc
      ) {
        this.search_loans();
      }
    },
    tab: function (newVal, oldVal) {
      if (newVal != oldVal) {
        this.options.page = 1;
        this.clearAll();
        this.loans = [];
        this.search_loans();
      }
    },
    searching: {
      deep: true,
      handler(val) {
        this.options.page = 1;
      },
    },
  },
  created() {
    this.search_loans();
    this.docsLoans();
  },

  methods: {
    async search_loans() {
      this.loading_table = true;
      try {
        let res = await axios.get(`list_loan_generate`, {
          params: {
            code_loan: this.searching.code_loan,
            identity_card_borrower: this.searching.identity_card_borrower,
            registration_borrower: this.searching.registration_borrower,
            full_name_borrower: this.searching.full_name_borrower,
            shortened_sub_modality_loan:
              this.searching.shortened_sub_modality_loan,
            state_type_affiliate: this.searching.state_type_affiliate,
            state_loan: this.tab == 0 ? "Vigente" : "Liquidado",
            guarantor_amortizing_loan: this.searching.guarantor_amortizing_loan,
            excel: false,
            page: this.options.page,
            per_page: this.options.itemsPerPage,
            sortBy: this.options.sortBy,
            sortDesc: this.options.sortDesc,
          },
        });
        console.log(res);
        this.loans = res.data.data;
        this.totalLoans = res.data.total;
        // delete res.data["data"];
        this.options.page = res.data.current_page;
        this.options.itemsPerPage = parseInt(res.data.per_page);
        this.loading_table = false;
      } catch (e) {
        console.log(e);
        this.loading_table = false;
      }
    },

    async download_loans() {
      this.loading_download = true;
      await axios({
        url: "/list_loan_generate",
        method: "GET",
        responseType: "blob", // important
        headers: { Accept: "application/vnd.ms-excel" },
        data: this.datos,
        params: {
          code_loan: this.searching.code_loan,
          identity_card_borrower: this.searching.identity_card_borrower,
          registration_borrower: this.searching.registration_borrower,
          full_name_borrower: this.searching.full_name_borrower,
          shortened_sub_modality_loan:
            this.searching.shortened_sub_modality_loan,
          state_type_affiliate: this.searching.state_type_affiliate,
          state_loan: this.tab == 0 ? "Vigente" : "Liquidado",
          guarantor_amortizing_loan: this.searching.guarantor_amortizing_loan,
          excel: true,
        },
      })
        .then((response) => {
          const url = window.URL.createObjectURL(new Blob([response.data]));
          const link = document.createElement("a");
          link.href = url;
          link.setAttribute("download", "ReportePrestamo.xls");
          document.body.appendChild(link);
          link.click();
          this.loading_download = false;
        })
        .catch((e) => {
          console.log(e);
          this.loading_download = false;
        });
    },

    clearAll() {
      (this.searching.code_loan = ""),
        (this.searching.identity_card_borrower = ""),
        (this.searching.registration_borrower = ""),
        (this.searching.full_name_borrower = ""),
        (this.searching.modality_loan = ""),
        (this.searching.shortened_sub_modality_loan = ""),
        (this.searching.state_type_affiliate = ""),
        (this.searching.guarantor_amortizing_loan = "");
      this.search_loans();
    },

    async imprimir(id, item) {
      try {
        let res;
        if (id == 1) {
          res = await axios.get(`loan/${item}/print/contract`);
        } else if (id == 2) {
          res = await axios.get(`loan/${item}/print/form`);
        } else if (id == 3) {
          res = await axios.get(`loan/${item}/print/plan`);
        } else {
          res = await axios.get(`loan/${item}/print/kardex`);
        }
        printJS({
          printable: res.data.content,
          type: res.data.type,
          documentTitle: res.data.file_name,
          base64: true,
        });
      } catch (e) {
        this.toastr.error("Ocurrió un error en la impresión.");
        console.log(e);
      }
    },

    docsLoans() {
      let docs = [];
      if (this.permissionSimpleSelected.includes("print-contract-loan")) {
        docs.push(
          { id: 1, title: "Contrato", icon: "mdi-file-document" },
          { id: 2, title: "Solicitud", icon: "mdi-file" }
        );
      }
      if (this.permissionSimpleSelected.includes("print-payment-plan")) {
        docs.push({ id: 3, title: "Plan de pagos", icon: "mdi-cash" });
      }
      if (this.permissionSimpleSelected.includes("print-payment-kardex-loan")) {
        docs.push({ id: 4, title: "Kardex", icon: "mdi-view-list" });
      } else {
        console.log(
          "Se ha producido un error durante la generación de la impresión"
        );
      }
      this.printDocs = docs;
    },

    _show_filter() {
      console.log("entra aca");
      this.show_filter = !this.show_filter;
    },
  },
};
</script>
<style scoped>
.v-text-field {
  background-color: white;
  width: 200px;
  padding: 5px;
  margin: 0px;
  font-size: 0.8em;
  border-color: teal;
}
.filter-text {
  font-size: 12px;
  height: 2px;
  margin: 0 0 40px 0;
  padding: 0;
  width: 100%;
}
</style>
