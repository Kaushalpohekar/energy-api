const mqtt = require('mqtt');
const { Client } = require('pg');

// PostgreSQL configuration
const pgClient2 = new Client({
    host: 'data.senselive.in',
    user: 'senselive',
    password: 'SenseLive@2025',
    database: 'senselive_db',
    port: 5432,
    ssl: { rejectUnauthorized: false },
});

pgClient2.connect((err) => {
    if (err) {
        console.error('Error connecting to PostgreSQL:', err.stack);
    } else {
        console.log('Connected to PostgreSQL database');
    }
});

// MQTT configuration
const mqttClient = mqtt.connect('ws://dashboard.senselive.in:9001', {
    username: 'Sense2023',
    password: 'sense123',
});

mqttClient.on('connect', () => {
    console.log('Connected to MQTT broker');

    const topics = [
        'Sense/Live/Energy/#',
        'EM6400NGVT110625/#',
        // 'Energy/Sense/Live/#'
    ];

    mqttClient.subscribe(topics, (err, granted) => {
        if (err) {
            console.error('MQTT subscription error:', err);
        } else {
            console.log('Subscribed to topics:', granted.map(g => g.topic).join(', '));
        }
    });
});

const dbFieldAliases = {
    device_uid: ['DeviceUID', 'device_uid', 'deviceUid', 'deviceid', 'deviceID', 'Device_ID', 'DEVICE_UID', 'UID', 'uid', 'Dev_UID', 'Device_Unique_Id', 'deviceUniqueId', 'meter_id', 'MeterID', 'DeviceSerial', 'DevID'],
    current_1: ['I_A', 'IA', 'Ia', 'i_a', 'iA', 'Current_A', 'CurrentA', 'PhaseA_Current', 'Phase_A_Current', 'A_Current', 'current_1'],
    current_2: ['I_B', 'IB', 'Ib', 'i_b', 'iB', 'Current_B', 'CurrentB', 'PhaseB_Current', 'Phase_B_Current', 'B_Current', 'current_2'],
    current_3: ['I_C', 'IC', 'Ic', 'i_c', 'iC', 'Current_C', 'CurrentC', 'PhaseC_Current', 'Phase_C_Current', 'C_Current', 'current_3'],
    current: ['I_Avg', 'I_Avg_L', 'I_Avg_L-L', 'I_Avg_LN', 'I_Avg_L-N', 'I_Avg_N', 'I_Avg_N-L', 'Current_Avg', 'CurrentAvg', 'Avg_Current', 'Average_Current', 'Average_Current_L', 'current', 'current_avg', 'current_avg_L', 'current_avg_LN', 'current_avg_L-N', 'current_avg_N', 'current_avg_N-L'],
    i_unblc_a: ['I_Unblc_A', 'i_unblc_a', 'IUnblcA', 'iUnblcA', 'I-Unblc-A', 'i-unblc-a', 'I_Unbalance_A', 'i_unbalance_a', 'IUnbalanceA', 'iUnbalanceA', 'Unbalance_I_A', 'unbalance_i_a', 'Unbal_I_A', 'unbal_i_a', 'Ia_unblc', 'ia_unblc', 'I_A_unblc', 'i_a_unblc', 'unbal_current_a', 'Unbal-Current-A'],
    i_unblc_b: ['I_Unblc_B', 'i_unblc_b', 'IUnblcB', 'iUnblcB', 'I-Unblc-B', 'i-unblc-b', 'I_Unbalance_B', 'i_unbalance_b', 'IUnbalanceB', 'iUnbalanceB', 'Unbalance_I_B', 'unbalance_i_b', 'Unbal_I_B', 'unbal_i_b', 'Ib_unblc', 'ib_unblc', 'I_B_unblc', 'i_b_unblc', 'unbal_current_b', 'Unbal-Current-B'],
    i_unblc_c: ['I_Unblc_C', 'i_unblc_c', 'IUnblcC', 'iUnblcC', 'I-Unblc-C', 'i-unblc-c', 'I_Unbalance_C', 'i_unbalance_c', 'IUnbalanceC', 'iUnbalanceC', 'Unbalance_I_C', 'unbalance_i_c', 'Unbal_I_C', 'unbal_i_c', 'Ic_unblc', 'ic_unblc', 'I_C_unblc', 'i_c_unblc', 'unbal_current_c', 'Unbal-Current-C'],
    exp_kwh: ['exp_kwh', 'kWh exp_in-L', 'kwh_exp_in_l', 'kwh-exp-in-l', 'KWH_EXP_IN_L', 'KwhExpInL', 'Export_kWh', 'export_kwh', 'kWh_export', 'kwhExport', 'exportEnergy_kWh', 'export_energy_kwh', 'Exp_kWh', 'expKWH', 'exp_kwh', 'kwhOut', 'kWhOut'],
    imp_kwh: ['imp_kwh', 'kWh imp_out-L', 'kwh_imp_out_l', 'kwh-imp-out-l', 'KWH_IMP_OUT_L', 'KwhImpOutL', 'Import_kWh', 'import_kwh', 'kWh_import', 'kwhImport', 'importEnergy_kWh', 'import_energy_kwh', 'Imp_kWh', 'impKWH', 'imp_kwh', 'kwhIn', 'kWhIn'],
    kwh: ['kwh', 'kWh exp+imp', 'kWh_exp_L', 'kwh_exp_imp', 'kwh-exp-imp', 'KWH_EXP_IMP', 'kWhExpImp', 'total_kwh', 'Total_kWh', 'kWh_Total', 'kwh_total_energy', 'energy_kwh', 'Energy_kWh', 'kWhSum', 'kwhSum', 'kwh_total', 'kwh_all', 'ActiveEnergyTotal_kWh', 'TotalEnergy_kWh'],
    exp_kvarh: ['kVARh_exp_in-L', 'kvarh_exp_in_l', 'kvarh-exp-in-l', 'KVARH_EXP_IN_L', 'kVArhExpInL', 'Export_kVARh', 'export_kvarh', 'kvarh_export', 'kvarhExport', 'exportReactiveEnergy_kVARh', 'export_reactive_kvarh', 'Exp_kVARh', 'expKVARh', 'exp_kvarh', 'kvarhOut', 'kVARhOut'],
    imp_kvarh: ['kVARh imp_out-L', 'kvarh_imp_out_l', 'kvarh-imp-out-l', 'KVARH_IMP_OUT_L', 'kVArhImpOutL', 'Import_kVARh', 'import_kvarh', 'kvarh_import', 'kvarhImport', 'importReactiveEnergy_kVARh', 'import_reactive_kvarh', 'Imp_kVARh', 'impKVARh', 'imp_kvarh', 'kvarhIn', 'kVARhIn'],
    kvarh: ['kVARh exp+imp', 'kVARh_exp_L', 'kvarh_exp_imp', 'kvarh-exp-imp', 'KVARH_EXP_IMP', 'kVArhExpImp', 'total_kvarh', 'Total_kVARh', 'kVARh_Total', 'kvarh_total_energy', 'reactive_energy_kvarh', 'ReactiveEnergy_kVARh', 'kvarhSum', 'kVARhSum', 'kvarh_total', 'kvarh_all', 'TotalReactive_kVARh', 'Total_kVARh_energy'],
    kvah: ['kVAh exp+imp', 'kVAh_exp_L', 'kvah_exp_imp', 'kvah-exp-imp', 'KVAH_EXP_IMP', 'kVAhExpImp', 'total_kvah', 'Total_kVAh', 'kVAh_Total', 'kvah_total_energy', 'apparent_energy_kvah', 'ApparentEnergy_kVAh', 'kvahSum', 'kVAhSum', 'kvah_total', 'kvah_all', 'TotalApparent_kVAh', 'Total_kVAh_energy'],
    wh_exp_in_l: ['Wh exp_in-L', 'wh_exp_in_l', 'wh-exp-in-l', 'WH_EXP_IN_L', 'WhExpInL', 'Export_Wh', 'export_wh', 'wh_export', 'exportWattHour', 'export_energy_wh', 'exp_wh', 'whOut', 'WhOut'],
    wh_imp_out_l: ['Wh imp_out-L', 'wh_imp_out_l', 'wh-imp-out-l', 'WH_IMP_OUT_L', 'WhImpOutL', 'Import_Wh', 'import_wh', 'wh_import', 'importWattHour', 'import_energy_wh', 'imp_wh', 'whIn', 'WhIn'],
    varh_exp: ['VARh_exp', 'varh_exp', 'varh-exp', 'VARH_EXP', 'ReactiveEnergy_Export', 'export_varh', 'varhExport', 'exportReactive_varh', 'varh_exp_kvar', 'varhExp', 'varh_exp_total', 'varh_exp_value', 'varh_exp'],
    varh_imp: ['VARh_imp', 'varh_imp', 'varh-imp', 'VARH_IMP', 'ReactiveEnergy_Import', 'import_varh', 'varhImport', 'importReactive_varh', 'varh_imp_kvar', 'varhImp', 'varh_imp_total', 'varh_imp_value', 'varh_imp'],
    vah_exp: ['VAh_exp', 'vah_exp', 'vah-exp', 'VAH_EXP', 'ApparentEnergy_Export', 'export_vah', 'vahExport', 'exportApparent_vah', 'vah_exp_kva', 'vahExp', 'vah_exp_total', 'vah_exp_value', 'vah_exp'],
    vah_imp: ['VAh_imp', 'vah_imp', 'vah-imp', 'VAH_IMP', 'ApparentEnergy_Import', 'import_vah', 'vahImport', 'importApparent_vah', 'vah_imp_kva', 'vahImp', 'vah_imp_total', 'vah_imp_value', 'vah_imp'],
    voltage_l: ['V_L-L Avg', 'V L-L Avg', 'V_L-L_Avg', 'VLLAvg', 'V_LL_Avg', 'VLLAVG', 'voltage_ll_avg', 'voltage_L-L', 'LineLineVoltage', 'voltage_l'],
    voltage_n: ['V_L-N Avg', 'V_L-N_Avg', 'V_L_N_Avg', 'V_Avg_N', 'V_LN_Avg', 'V_LNAVG', 'VLNAvg', 'voltage_ln_avg', 'V_LtoN_Avg', 'LineToNeutralVoltage', 'voltage_n'],
    voltage_1n: ['V_A-N', 'V_A_N', 'VA-N', 'VA_N', 'Voltage_A_N', 'Voltage_A-N', 'PhaseA_Voltage_N', 'Voltage_AN', 'V_A_to_N', 'voltage_1n'],
    voltage_2n: ['V_B-N', 'V_B_N', 'VB-N', 'VB_N', 'Voltage_B_N', 'Voltage_B-N', 'PhaseB_Voltage_N', 'Voltage_BN', 'V_B_to_N', 'voltage_2n'],
    voltage_3n: ['V_C-N', 'V_C_N', 'VC-N', 'VC_N', 'Voltage_C_N', 'Voltage_C-N', 'PhaseC_Voltage_N', 'Voltage_CN', 'V_C_to_N', 'voltage_3n'],
    v_unblc_a_b: ['V_Unblc_A-B', 'V_Unblc_A_B', 'v_unblc_a_b', 'VUNBLC_A_B', 'VUnblcAB', 'VoltageUnbalanceAB', 'unbal_voltage_a_b', 'UnbalVoltageAB', 'v-unblc-a-b', 'v_unblc_a_b'],
    v_unblc_b_c: ['V_Unblc_B-C', 'V_Unblc_B_C', 'v_unblc_b_c', 'VUNBLC_B_C', 'VUnblcBC', 'VoltageUnbalanceBC', 'unbal_voltage_b_c', 'UnbalVoltageBC', 'v-unblc-b-c', 'v_unblc_b_c'],
    v_unblc_c_a: ['V_Unblc_C-A', 'V_Unblc_C_A', 'v_unblc_c_a', 'VUNBLC_C_A', 'VUnblcCA', 'VoltageUnbalanceCA', 'unbal_voltage_c_a', 'UnbalVoltageCA', 'v-unblc-c-a', 'v_unblc_c_a'],
    v_unblc_a_n: ['V_Unblc_A-N', 'V_Unblc_A_N', 'v_unblc_a_n', 'VUNBLC_A_N', 'VUnblcAN', 'VoltageUnbalanceAN', 'unbal_voltage_a_n', 'UnbalVoltageAN', 'v-unblc-a-n', 'v_unblc_a_n'],
    v_unblc_b_n: ['V_Unblc_B-N', 'V_Unblc_B_N', 'v_unblc_b_n', 'VUNBLC_B_N', 'VUnblcBN', 'VoltageUnbalanceBN', 'unbal_voltage_b_n', 'UnbalVoltageBN', 'v-unblc-b-n', 'v_unblc_b_n'],
    v_unblc_c_n: ['V_Unblc_C-N', 'V_Unblc_C_N', 'v_unblc_c_n', 'VUNBLC_C_N', 'VUnblcCN', 'VoltageUnbalanceCN', 'unbal_voltage_c_n', 'UnbalVoltageCN', 'v-unblc-c-n', 'v_unblc_c_n'],
    pf_1: ['PF_A', 'pf_a', 'PowerFactor_A', 'power_factor_a', 'Power_Factor_A', 'pf-a', 'PF-A', 'PhaseA_PF', 'pf1', 'pf_1'],
    pf_2: ['PF_B', 'pf_b', 'PowerFactor_B', 'power_factor_b', 'Power_Factor_B', 'pf-b', 'PF-B', 'PhaseB_PF', 'pf2', 'pf_2'],
    pf_3: ['PF_C', 'pf_c', 'PowerFactor_C', 'power_factor_c', 'Power_Factor_C', 'pf-c', 'PF-C', 'PhaseC_PF', 'pf3', 'pf_3'],
    pf: ['PF_Avg', 'pf_avg', 'PFAVG', 'Average_PF', 'AvgPF', 'avg_pf', 'PowerFactor_Avg', 'Power_Factor_Total', 'pf_total', 'pf'],
    freq: ['F', 'f', 'Frequency', 'frequency', 'Freq', 'Freq_Hz', 'frequency_hz', 'Hz', 'hz', 'freq'],
    voltage_12: ['V_A-B', 'V_A_B', 'VA-B', 'VA_B', 'Voltage_AB', 'Voltage_A_B', 'LineVoltage_AB', 'voltage_ab', 'voltage_12'],
    voltage_23: ['V_B-C', 'V_B_C', 'VB-C', 'VB_C', 'Voltage_BC', 'Voltage_B_C', 'LineVoltage_BC', 'voltage_bc', 'voltage_23'],
    voltage_31: ['V_C-A', 'V_C_A', 'VC-A', 'VC_A', 'Voltage_CA', 'Voltage_C_A', 'LineVoltage_CA', 'voltage_ca', 'voltage_31'],
    kw_1: ['kw_A', 'KW_A', 'kW_A', 'KW-A', 'kW-A', 'kwA', 'KW_A_Power', 'ActivePower_A', 'kw_a', 'kw_1', 'kw _A'],
    kw_2: ['kw_B', 'KW_B', 'kW_B', 'KW-B', 'kW-B', 'kwB', 'KW_B_Power', 'ActivePower_B', 'kw_b', 'kw_2', 'kw _B'],
    kw_3: ['kw_C', 'KW_C', 'kW_C', 'KW-C', 'kW-C', 'kwC', 'KW_C_Power', 'ActivePower_C', 'kw_c', 'kw_3', 'kw _C'],
    kw: ['kw_Avg', 'kW_Avg', 'KW_Avg', 'KWAVG', 'Avg_kW', 'Average_kW', 'kw_avg', 'kw-total', 'kwTotal', 'Total_kW', 'kw', 'ActivePowerTotal'],
    kvar_1: ['kVAr_A', 'KVAR_A', 'kvar_A', 'kVAR-A', 'kVAR_A_Phase', 'ReactivePower_A', 'kvarA', 'kvar_1'],
    kvar_2: ['kVAr_B', 'KVAR_B', 'kvar_B', 'kVAR-B', 'kVAR_B_Phase', 'ReactivePower_B', 'kvarB', 'kvar_2'],
    kvar_3: ['kVAr_C', 'KVAR_C', 'kvar_C', 'kVAR-C', 'kVAR_C_Phase', 'ReactivePower_C', 'kvarC', 'kvar_3'],
    kvar: ['kVAr_Avg', 'KVAr_Avg', 'KVAR_Avg', 'kvar_avg', 'kvar_Avg', 'kVARAVG', 'Avg_kVAR', 'kvarTotal', 'ReactivePowerTotal', 'kvar'],
    kva_1: ['kVA_A', 'KVA_A', 'kva_A', 'KVA-A', 'ApparentPower_A', 'Apparent_Power_A', 'kvaA', 'KvaA', 'kva_1'],
    kva_2: ['kVA_B', 'KVA_B', 'kva_B', 'KVA-B', 'ApparentPower_B', 'Apparent_Power_B', 'kvaB', 'KvaB', 'kva_2'],
    kva_3: ['kVA_C', 'KVA_C', 'kva_C', 'KVA-C', 'ApparentPower_C', 'Apparent_Power_C', 'kvaC', 'KvaC', 'kva_3'],
    kva: ['kVA_Avg', 'KVA_Avg', 'KVA avg', 'kva avg', 'kva_avg', 'Kva_Avg', 'KVA-Avg', 'kva-avg', 'Avg_KVA', 'Average_KVA', 'average_kva', 'AVG_KVA', 'AVG kva', 'avg_kva', 'kvaTotal', 'ApparentPowerTotal', 'kva'],
    min_kw: ['kW_Lst_D', 'KW_Lst_D', 'kw_lst_d', 'KW-LST-D', 'MinKW', 'min_kw', 'kw_min_d', 'KW_Min_D', 'KW-Min-D', 'Min_kW'],
    min_kva: ['kVA_Lst_D', 'KVA_Lst_D', 'kva_lst_d', 'KVA-LST-D', 'MinKVA', 'min_kva', 'kva_min_d', 'KVA_Min_D', 'KVA-Min-D', 'Min_kVA'],
    min_kvar: ['kVAR_Lst_D', 'KVAR_Lst_D', 'kvar_lst_d', 'KVAR-LST-D', 'MinKVAR', 'min_kvar', 'kvar_min_d', 'KVAR_Min_D', 'KVAR-Min-D', 'Min_kVAR'],
    max_kw: ['kW_Peak_D', 'KW_Peak_D', 'kw_peak_d', 'KW-PEAK-D', 'MaxKW', 'PeakKW', 'max_kw'],
    max_kvar: ['kVAR_Peak_D', 'KVAR_Peak_D', 'kvar_peak_d', 'KVAR-PEAK-D', 'MaxKVAR', 'PeakKVAR', 'max_kvar'],
    max_kva: ['kVA_Pk_D', 'KVA_Pk_D', 'kva_pk_d', 'KVA-PK-D', 'MaxKVA', 'PeakKVA', 'max_kva'],
    thd_i1: ['THD_I_A', 'THD-IA', 'thd_i_a', 'ThdIA', 'THDI_A', 'THDIA', 'THDCurrentA', 'thd_i1', 'THD I_A', 'THD I A', 'THD_I_A', 'THD-I-A', 'thd i a', 'thd-i-a', 'thd i_a', 'thd-i_a', 'thdcurrenta'],
    thd_i2: ['THD_I_B', 'THD-IB', 'thd_i_b', 'ThdIB', 'THDI_B', 'THDIB', 'THDCurrentB', 'thd_i2', 'THD I_B', 'THD I B', 'THD_I_B', 'THD-I-B', 'thd i b', 'thd-i-b', 'thd i_b', 'thd-i_b', 'thdcurrentb'],
    thd_i3: ['THD_I_C', 'THD-IC', 'thd_i_c', 'ThdIC', 'THDI_C', 'THDIC', 'THDCurrentC', 'thd_i3', 'THD I_C', 'THD I C', 'THD_I_C', 'THD-I-C', 'thd i c', 'thd-i-c', 'thd i_c', 'thd-i_c', 'thdcurrentc'],
    thd_v12: ['THD_V_A-B', 'THD-V-AB', 'THDVAB', 'THDVoltageAB', 'thd_v_ab', 'thd_v12', 'THD V_A-B', 'THD V A-B', 'THD V A B', 'THD_V_A_B', 'THD-V-A-B', 'thd v a b', 'thd-v-a-b', 'thd v_a_b', 'thd-v_a_b', 'thdvoltageab'],
    thd_v23: ['THD_V_B-C', 'THD-V-BC', 'THDVBC', 'THDVoltageBC', 'thd_v_bc', 'thd_v23', 'THD V_B-C', 'THD V B-C', 'THD V B C', 'THD_V_B_C', 'THD-V-B-C', 'thd v b c', 'thd-v-b-c', 'thd v_b_c', 'thd-v_b_c', 'thdvoltagebc'],
    thd_v31: ['THD_V_C-A', 'THD-V-CA', 'THDVCA', 'THDVoltageCA', 'thd_v_ca', 'thd_v31', 'THD V_C-A', 'THD V C-A', 'THD V C A', 'THD_V_C_A', 'THD-V-C-A', 'thd v c a', 'thd-v-c-a', 'thd v_c_a', 'thd-v_c_a', 'thdvoltageca'],
    thd_v1n: ['THD_V_A-N', 'THD-V-AN', 'THDVAN', 'THDVoltageAN', 'thd_v_an', 'thd_v1n', 'THD V_A-N', 'THD V A-N', 'THD V A N', 'THD V_A_N', 'THD-V-A-N', 'thd v_a-n', 'thd v a-n', 'thd v a n', 'thd v_a_n', 'thd-v-a-n', 'thdvan', 'thd voltage an', 'thd voltage_a_n', 'thd-voltage-a-n'],
    thd_v2n: ['THD_V_B-N', 'THD-V-BN', 'THD_VBN', 'THDVoltageBN', 'thd_v_bn', 'thd_v2n', 'THD V_B-N', 'THD V B-N', 'THD V B N', 'THD V_B_N', 'THD-V-B-N', 'thd v_b-n', 'thd v b-n', 'thd v b n', 'thd v_b_n', 'thd-v-b-n', 'thdvbn', 'thd voltage bn', 'thd voltage_b_n', 'thd-voltage-b-n'],
    thd_v3n: ['THD_V_C-N', 'THD-V-CN', 'THDVCN', 'THDVoltageCN', 'thd_v_cn', 'thd_v3n', 'THD V_C-N', 'THD V C-N', 'THD V C N', 'THD V_C_N', 'THD-V-C-N', 'thd v_c-n', 'thd v c-n', 'thd v c n', 'thd v_c_n', 'thd-v-c-n', 'thdvcn', 'thd voltage cn', 'thd voltage_c_n', 'thd-voltage-c-n'],
};

const fieldAliasMap = {};
for (const [dbField, aliases] of Object.entries(dbFieldAliases)) {
    aliases.forEach(alias => {
        fieldAliasMap[alias.trim()] = dbField;
    });
}

const dbColumns = [
    'device_uid',
    'voltage_1n', 'voltage_2n', 'voltage_3n', 'voltage_n',
    'voltage_12', 'voltage_23', 'voltage_31', 'voltage_l',
    'current_1', 'current_2', 'current_3', 'current',
    'kw_1', 'kw_2', 'kw_3', 'kvar_1', 'kvar_2', 'kvar_3', 'kva_1', 'kva_2', 'kva_3',
    'pf_1', 'pf_2', 'pf_3', 'pf', 'freq', 'kw', 'kvar', 'kva',
    'max_kw', 'min_kw', 'max_kvar', 'min_kvar', 'max_kva',
    'max_int_v1n', 'max_int_v2n', 'max_int_v3n',
    'max_int_v12', 'max_int_v23', 'max_int_v31',
    'max_int_i1', 'max_int_i2', 'max_int_i3',
    'imp_kwh', 'exp_kwh', 'kwh', 'imp_kvarh', 'exp_kvarh', 'kvarh', 'kvah',
    'run_h', 'on_h',
    'thd_v1n', 'thd_v2n', 'thd_v3n', 'thd_v12', 'thd_v23', 'thd_v31',
    'thd_i1', 'thd_i2', 'thd_i3', 'thd_i_n', 'thd_v_l_l', 'thd_v_l_n',
    'ser_no', 'status',
    'i_unblc_a', 'i_unblc_b', 'i_unblc_c',
    'kwh_exp_imp', 'kvarh_exp_imp', 'kvah_exp_in_l', 'kvah_imp_out_l', 'kvah_exp_imp',
    'wh_exp_in_l', 'wh_imp_out_l', 'wh_expplusimp', 'wh_exp_imp',
    'varh_exp', 'varh_imp', 'varh_expplusimp', 'varh_exp_imp',
    'vah_exp', 'vah_imp', 'vah_expplusimp', 'vah_exp_imp',
    'v_unblc_a_b', 'v_unblc_b_c', 'v_unblc_c_a',
    'v_unblc_a_n', 'v_unblc_b_n', 'v_unblc_c_n',
    'kw_psnt_d', 'kw_pred_d', 'kvar_prsnt_d', 'kvar_prdct_d',
    'min_kva', 'kva_prsnt_d', 'kva_prdct_d',
    'lst_d_i', 'prsnt_d_i', 'prdct_d_i', 'pk_d_i',
    ...Array.from({ length: 15 }, (_, i) => `vab_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `vbc_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `vca_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `van_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `vbn_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `vcn_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `ia_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `ib_h_${i + 1}`),
    ...Array.from({ length: 15 }, (_, i) => `ic_h_${i + 1}`)
];


const insertQuery = `
INSERT INTO ems_schema.ems_actual_data (
  ${dbColumns.join(', ')}, date_time
) VALUES (
  ${dbColumns.map((_, i) => `$${i + 1}`).join(', ')}, $${dbColumns.length + 1}
)`;

mqttClient.on('message', (topic, message) => {
    try {
        const raw = JSON.parse(message);
        const data = {};

        console.log(`[${new Date().toISOString()}] Received message on topic: ${topic}`);
        for (const key in raw) {
            const cleanKey = key.trim();
            const mappedKey = fieldAliasMap[cleanKey] || cleanKey;
            data[mappedKey] = raw[key];
        }

        console.log(`\n📦 Insertable fields for DeviceUID: ${data.device_uid || 'Unknown'}`);
        dbColumns.forEach(col => {
            console.log(`→ ${col}: ${data[col]}`);
        });

        const values = dbColumns.map((col) => data[col] ?? null);
        values.push(new Date());

        if (data.device_uid) {
            pgClient2.query(insertQuery, values)
                .then(() => {
                    //(`[${new Date().toISOString()}] Data inserted for ${data.device_uid}`);
                })
                .catch((err) => {
                    console.error('PostgreSQL INSERT error:', err.message);
                });
        } else {
            console.warn('Device UID missing, skipping insert');
        }
    } catch (err) {
        console.error('MQTT message processing error:', err.message);
    }
});

mqttClient.on('error', (err) => {
    console.error('MQTT error:', err.message);
});
