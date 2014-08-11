/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ttr.tt.sms;

import btd.sequence.exception.EndSequence;
import btd.sequence.exception.NoSuchSequence;
import btd.sequence.exception.UnableToUseSequence;
import com.objsys.asn1j.runtime.Asn1Boolean;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.SQLException;
import java.util.List;
import ttr.asn1.CHARGINGCDR.DccCorrelationID;
import ttr.asn1.CHARGINGCDR.OnlineCreditControlRecord;
import ttr.asn1.CHARGINGCDR.SubscriptionID;
import ttr.asn1.CreditControlDataTypes.CCAccountData;
import ttr.asn1.CreditControlDataTypes.ContextParameter;
import ttr.asn1.CreditControlDataTypes.ContextParameterValueType;
import ttr.asn1.CreditControlDataTypes.CreditControlRecord;
import ttr.asn1.CreditControlDataTypes.MonetaryUnits;
import ttr.asn1.CreditControlDataTypes.SDPCreditControlRecord;
import ttr.asn1.CreditControlDataTypes.TreeDefinedField;
import ttr.asn1.CreditControlDataTypes.UsedServiceUnit;
import ttr.tt.TrafficType;
import ttr.utils.Const;
import ttr.utils.Util;

/**
 *
 * @author b482384
 */
public class SMS implements TrafficType {

    private final String FORMAT = "%-202s";
    private final String SEQUENCE = Const.SMS_SEQ;
    private final String DPREFIX = "EVDTL";
    private final String CPREFIX = "EVCTL";
    private BufferedWriter detail;
    private String tmpFileName;
    private String seqNum;
    private String firstRecord = null;
    private String lastRecord = null;
    private boolean firsTime = true;
    private int cdrs;
    public boolean error = false;
    // Particulares de SMS
    private final long MO_300 = 300;
    private final long MO_320 = 320;
    private final long MT_340 = 340;
    private final long CP_400 = 400;
    private final long CP_500 = 500;
    private final long CP_600 = 600;
    private final long CTXT_ORIGIN = 16777416l;
    private final long CTXT_VLR = 16778217l;
    private final long CTXT_DEST = 16777417l;
    private final long CTXT_TARIFF = 16777416l;
    private final String TDF_SOLIDARIO = "TDF_Solidario";
    private final String TDF_VAS_TARIFF_CATEGORY = "VASTariffCategory";
    private final String TDF_COST_VA = "TDF_Cost VA";

    public SMS() throws IOException, UnableToUseSequence, ClassNotFoundException, SQLException, NoSuchSequence, EndSequence {
        this.cdrs = 0;
        seqNum = Util.getSequence(Const.TMP_SEQ);
        tmpFileName = Const.ACM_OUT + "tmp." + DPREFIX + seqNum;
        detail = new BufferedWriter(new FileWriter(tmpFileName));
    }

    @Override
    public void setOutputCDR(OnlineCreditControlRecord occ) throws IOException {

        final String operation = "DELIVERYOK";
        String origin = "";
        String vlrNumber = "";
        String destination = "";
        long tariff = 0;
        String carrier = "";
        String dateOfArrivalSMSC;
        String timeOfArrivalSMSC;
        String dateOfEventSMSC;
        String timeOfEventSMSC;
        String reference = "";
        String dateOfDebit;
        String timeOfDebit;
        String tariffAtOrigin = "N";
        long transportAtOrigin = 0; // largo 8, 6 enteros y 2 decimales
        long addedValueAtOrigin = 0; // largo 8, 6 enteros y 2 decimales
        String tariffAtDestination = "N";
        long transportAtDestination = 0; // largo 8, 6 enteros y 2 decimales
        long addedValueAtDestination = 0; // largo 8, 6 enteros y 2 decimales

        String actRecord = null;
        boolean discard = false;

        if (occ != null && occ.creditControlRecords != null) {
            SDPCreditControlRecord[] listSDP = occ.creditControlRecords.elements;
            CreditControlRecord ccr = (CreditControlRecord) listSDP[0].getElement();
            ContextParameter[] c = ccr.chargingContextSpecific.elements;
            SubscriptionID[] s = occ.servedSubscriptionID.elements;
            TreeDefinedField[] t = null;
            if (ccr.treeDefinedFields != null) {
                t = ccr.treeDefinedFields.elements;
            }
            CCAccountData cca = null;
            if (ccr.cCAccountData != null) {
                cca = ccr.cCAccountData;
            }
            if (isNotReimbursement(occ, ccr)) {
                // -- hay que tratar de descartar antes el cdr por el tema de los reembolsos.
                // origen
                if (ccr.serviceIdentifier.value == this.MO_300 || ccr.serviceIdentifier.value == this.MO_320
                        || ccr.serviceIdentifier.value == this.CP_400 || ccr.serviceIdentifier.value == this.CP_500
                        || ccr.serviceIdentifier.value == this.CP_600) {
                    origin = s[0].subscriptionIDValue.value;
                } else {
                    if (ccr.serviceIdentifier.value == this.MT_340) {
                        origin = getValue(c, CTXT_ORIGIN);
                    } else {
                        discard = true;
                    }
                }
                // portadora, 
                if (ccr.serviceIdentifier.value == this.MO_300 || ccr.serviceIdentifier.value == this.MO_320
                        || ccr.serviceIdentifier.value == this.MT_340) {
                    carrier = "SMC";
                } else {
                    if (ccr.serviceIdentifier.value == this.CP_400) {
                        carrier = "MMS";
                    } else {
                        if (ccr.serviceIdentifier.value == this.CP_500) {
                            carrier = "WAP";
                        } else {
                            if (ccr.serviceIdentifier.value == this.CP_600) {
                                carrier = "SMS";
                            } else {
                                discard = true;
                            }
                        }
                    }
                }
                // VLR
                if (carrier.equals("SMC")) {
                    vlrNumber = getValue(c, CTXT_VLR);
                    tariff = 0l;
                } else {
                    vlrNumber = "";
                    String aux = getValue(c, CTXT_TARIFF);
                    if (aux.isEmpty()) {
                        aux = "0";
                    }
                    tariff = Long.parseLong(aux);
                }
                // Destino
                if (ccr.serviceIdentifier.value == this.CP_400 || ccr.serviceIdentifier.value == this.CP_500
                        || ccr.serviceIdentifier.value == this.CP_600) {
                    destination = ""; // valido
                } else {
                    if (ccr.serviceIdentifier.value == this.MO_300 || ccr.serviceIdentifier.value == this.MO_320) {
                        destination = getValue(c, CTXT_DEST);
                    } else {
                        if (ccr.serviceIdentifier.value == this.MT_340) {
                            destination = s[0].subscriptionIDValue.value;
                        } else {
                            discard = true;
                        }

                    }
                }
                String eventTime = ccr.eventTime.value;
                // dates & times
                dateOfArrivalSMSC = eventTime;
                timeOfArrivalSMSC = eventTime;
                dateOfEventSMSC = eventTime;
                timeOfEventSMSC = eventTime;
                // reference + tariff
                if ("dccCorrelationId".equals(occ.correlationID.getElemName())) {
                    DccCorrelationID d = (DccCorrelationID) occ.correlationID.getElement();
                    reference = d.sessionId.value;
                    String pre = "";
                    if (carrier.equals("SMC")) {
                        pre = "00";
                    } else {
                        pre = "03";
                    }
                    reference = pre + reference.substring(reference.length() - 10);
                }
                // more dates & times
                dateOfDebit = eventTime;
                timeOfDebit = eventTime;
                // tariffAtOrigin, tariffAtDestination
                String tdfSolidario = getTDFValue(t, this.TDF_SOLIDARIO);
                if (tdfSolidario.isEmpty()) {
                    tdfSolidario = "FALSE";
                }
                tariffAtOrigin = getTariff("origin", ccr.serviceIdentifier.value, ccr.cCAccountData.serviceClassID.value, tdfSolidario);
                tariffAtDestination = getTariff("destination", ccr.serviceIdentifier.value, ccr.cCAccountData.serviceClassID.value, tdfSolidario);
                String vasCategory = getTDFValue(t, this.TDF_VAS_TARIFF_CATEGORY);
                String aux = getTDFValue(t, this.TDF_COST_VA);
                if (aux.isEmpty()) {
                    aux = "0";
                }
                long tdfCost = Long.parseLong(aux);

                transportAtOrigin = getTransport("tr_origin", ccr.serviceIdentifier.value, tdfCost, cca.accumulatedCost, vasCategory);
                addedValueAtOrigin = getTransport("va_origin", ccr.serviceIdentifier.value, tdfCost, cca.accumulatedCost, vasCategory);
                transportAtDestination = getTransport("tr_destination", ccr.serviceIdentifier.value, tdfCost, cca.accumulatedCost, vasCategory);
                addedValueAtDestination = getTransport("va_destination", ccr.serviceIdentifier.value, tdfCost, cca.accumulatedCost, vasCategory);

                actRecord = formatSMS(operation, origin, vlrNumber, destination, tariff, carrier, dateOfArrivalSMSC, timeOfArrivalSMSC,
                        dateOfEventSMSC, timeOfEventSMSC, reference, dateOfDebit, timeOfDebit, tariffAtOrigin,
                        transportAtOrigin, addedValueAtOrigin, tariffAtDestination, transportAtDestination, addedValueAtDestination);
            }
        } else {
            this.error = true;
        }
        // ------------------------ Se graba 
        if (actRecord != null && !this.error && !discard) {
            this.lastRecord = actRecord;
            if (this.firsTime) {
                this.firstRecord = this.lastRecord;
                this.firsTime = false;
            }
            detail.write(actRecord);
            cdrs++;
        }
    }

    @Override
    public void close() throws IOException, UnableToUseSequence, ClassNotFoundException, SQLException, NoSuchSequence, EndSequence {
        detail.close();
        if (firstRecord != null) {
            seqNum = Util.getSequence(SEQUENCE);
            String date = Util.fechaHoy();
            String records = String.format("%011d", cdrs) + "";

            Path DIRBASE = Paths.get(Const.ACM_OUT);

            Path tmp = DIRBASE.resolve(tmpFileName);
            Path acm = Paths.get(Const.ACM_OUT + DPREFIX + seqNum);
            Path aic = Paths.get(Const.AIC_OUT + DPREFIX + seqNum);

            Files.move(tmp, acm, StandardCopyOption.REPLACE_EXISTING);
            Files.copy(acm, aic, StandardCopyOption.REPLACE_EXISTING);

            // archivo de control
            BufferedWriter control = new BufferedWriter(new FileWriter(Const.ACM_OUT + CPREFIX + seqNum));

            control.write(String.format(FORMAT, date) + Const.ENTER);
            control.write(firstRecord);
            control.write(lastRecord);
            control.write(String.format(FORMAT, records) + Const.ENTER);
            control.close();

            Path cacm = Paths.get(Const.ACM_OUT + CPREFIX + seqNum);
            Path caic = Paths.get(Const.AIC_OUT + CPREFIX + seqNum);
            Files.copy(cacm, caic, StandardCopyOption.REPLACE_EXISTING);
        } else {
            Util.deleteFile(tmpFileName);
        }
    }

    @Override
    public boolean getError() {
        return this.error;
    }

    @Override
    public int getCDRS() {
        return this.cdrs;
    }

    @Override
    public String traceFile(List<String> lotFiles) {
        for (String consolidatedFile : lotFiles) {
            System.err.println(consolidatedFile + "," + DPREFIX + seqNum);
        }
        return DPREFIX + seqNum;
    }

    private String getValue(ContextParameter[] c, Long CTXT_ID) {
        String value = "";
        for (ContextParameter cp : c) {
            if (cp != null && cp.parameterValue != null) {
                if (cp.parameterID.value == CTXT_ID) {
                    //System.out.println("ID: " + CTXT_ID + " tipo: " + cp.parameterValue.getChoiceID());
                    switch (cp.parameterValue.getChoiceID()) {
                        case ContextParameterValueType._BOOLEAN_:
                            if (cp.parameterValue.getElement().equals(true)) {
                                value = "TRUE";
                            } else {
                                value = "FALSE";
                            }
                            break;
                        case ContextParameterValueType._INTEGER32:
                            value = cp.parameterValue.getElement().toString();
                            break;
                        case ContextParameterValueType._UNSIGNED32:
                            value = cp.parameterValue.getElement().toString();
                            break;
                        case ContextParameterValueType._OCTETSTRING:
                            value = cp.parameterValue.getElement().toString();
                            break;
                        case ContextParameterValueType._STRING:
                            value = cp.parameterValue.getElement().toString();
                            break;
                    }
                }
            }
        }
        return value;
    }

    private String getTDFValue(TreeDefinedField[] t, String TDF_REF) {

        String value = "";

        if (t != null) {
            for (TreeDefinedField treeDefinedField : t) {
                if (treeDefinedField != null && treeDefinedField.parameterValue != null) {
                    if (treeDefinedField.parameterID.value.equals(TDF_REF)) {
                        switch (treeDefinedField.parameterValue.getChoiceID()) {
                            case TreeDefinedField.BOOLEAN:
                                if (((Asn1Boolean)treeDefinedField.parameterValue.getElement()).value) {
                                    value = "TRUE";
                                } else {
                                    value = "FALSE";
                                }
                                break;
                            case TreeDefinedField.INTEGER:
                                value = treeDefinedField.parameterValue.getElement().toString();
                                break;
                        }
                    }
                }
            }
        }
        return value;
    }

    private String getTariff(String kind, long serviceIdentifier, long serviceClassID, String tdfSolidario) {

        String tariff = "N";
        String tariffOrigin = "N";
        String tariffDestination = "N";

        if (serviceIdentifier == this.MO_300 || serviceIdentifier == this.MO_320 || serviceIdentifier == this.CP_400
                || serviceIdentifier == this.CP_500 || serviceIdentifier == this.CP_600) {
            if (serviceClassID >= 4000 && serviceClassID <= 4999) {
                tariffOrigin = "N";
                tariffDestination = "N";
            } else {
                if (tdfSolidario.equals("TRUE")) {
                    tariffOrigin = "N";
                    tariffDestination = "N";
                } else {
                    tariffOrigin = "S";
                    tariffDestination = "N";
                }
            }
        } else {
            if (serviceIdentifier == this.MT_340) {
                if (serviceClassID >= 4000 && serviceClassID <= 4999) {
                    tariffOrigin = "N";
                    tariffDestination = "N";
                } else {
                    if (tdfSolidario.equals("TRUE")) {
                        tariffOrigin = "N";
                        tariffDestination = "N";
                    } else {
                        tariffOrigin = "N";
                        tariffDestination = "S";
                    }
                }
            }
        }
        switch (kind) {
            case "origin":
                tariff = tariffOrigin;
                break;
            case "destination":
                tariff = tariffDestination;
                break;
        }
        return tariff;
    }

    private long getTransport(String kind, long serviceIdentifier, long tdfCost,
            MonetaryUnits accumulatedCost, String vasCategory) {

        long value = 0l;
        long ac = getAC(accumulatedCost);

        switch (vasCategory) {
            case "1":
                if (kind.equals("tr_origin") && (serviceIdentifier == this.MO_300 || serviceIdentifier == this.MO_320)) {
                    value = ac - tdfCost;

                }
                break;
            case "2":
                if (kind.equals("va_origin") && (serviceIdentifier == this.MO_300 || serviceIdentifier == this.MO_320)) {
                    value = tdfCost;
                }
                if (kind.equals("tr_origin") && (serviceIdentifier == this.MO_300 || serviceIdentifier == this.MO_320)) {
                    value = ac - tdfCost;

                }
                break;
            case "3":
                if (kind.equals("tr_destination") && (serviceIdentifier == this.MT_340)) {
                    value = ac;
                }
                break;
            case "4":
                if (kind.equals("va_destination") && (serviceIdentifier == this.MT_340)) {
                    value = ac;
                }
                break;
            case "5":
                if (kind.equals("tr_origin") && (serviceIdentifier == this.CP_400 || serviceIdentifier == this.CP_500
                        || serviceIdentifier == this.CP_600)) {
                    value = ac;
                }
                break;
            case "6":
                if (kind.equals("va_origin") && (serviceIdentifier == this.CP_400 || serviceIdentifier == this.CP_500
                        || serviceIdentifier == this.CP_600)) {
                    value = ac;
                }
                break;

        }

        return value;
    }

    private boolean isNotReimbursement(OnlineCreditControlRecord occ, CreditControlRecord ccr) {

        boolean notReimbursement = true;

        if ("SCAP_V.2.0@ericsson.com".equals(occ.serviceContextID.value)) {
            if (ccr.serviceIdentifier.value == CP_400 || ccr.serviceIdentifier.value == CP_500) {
                if (ccr.usedServiceUnits != null) {
                    UsedServiceUnit[] u = ccr.usedServiceUnits.elements;
                    if (u != null && u[0].serviceSpecificUnit != null) {
                        if (u[0].serviceSpecificUnit.value.intValue() != 0) {
                            notReimbursement = true;
                        } else {
                            notReimbursement = false;
                        }
                    }
                }
            }

        }
        return notReimbursement;
    }

    private long getAC(MonetaryUnits accumulatedCost) {
        long ac = 0l;

        /* 
         accumulatedCost {
         amount = 5871616
         decimals = 6
         currency = 858
         }
         float divisor = ccr.cCAccountData.accumulatedCost.decimals.value - 2;
         divisor = (float) Math.pow(10.0, divisor);
         valorAOrigen = Math.round(ccr.cCAccountData.accumulatedCost.amount.value / divisor);
         Si ac = 5,871616 --> ac = 587 --> por eso el "-2" y luego el round
         */
        float divisor = accumulatedCost.decimals.value - 2;
        divisor = (float) Math.pow(10.0, divisor);
        ac = Math.round(accumulatedCost.amount.value / divisor);

        return ac;
    }

    private String formatSMS(String operation, String origin, String vlrNumber,
            String destination, long tariff, String carrier, String dateOfArrivalSMSC,
            String timeOfArrivalSMSC, String dateOfEventSMSC, String timeOfEventSMSC,
            String reference, String dateOfDebit, String timeOfDebit, String tariffAtOrigin,
            long transportAtOrigin, long addedValueAtOrigin, String tariffAtDestination,
            long transportAtDestination, long addedValueAtDestination) {

        return String.format("%-202s", (String.format("%-15s", operation) + Const.SPACE
                + String.format("%-20s", origin) + Const.SPACE
                + String.format("%-20s", vlrNumber) + Const.SPACE
                + String.format("%-20s", destination) + Const.SPACE
                + String.format("%04d", tariff) + Const.SPACE
                + String.format("%-3s", carrier) + Const.SPACE
                + formatDate(dateOfArrivalSMSC) + Const.SPACE
                + formatTime(timeOfArrivalSMSC) + Const.SPACE
                + formatDate(dateOfEventSMSC) + Const.SPACE
                + formatTime(timeOfEventSMSC) + Const.SPACE
                + String.format("%12s", reference) + Const.SPACE
                + formatDate(dateOfDebit) + Const.SPACE
                + formatTime(timeOfDebit) + Const.SPACE
                + tariffAtOrigin + Const.SPACE
                + String.format("%08d", transportAtOrigin) + Const.SPACE
                + String.format("%08d", addedValueAtOrigin) + Const.SPACE
                + tariffAtDestination + Const.SPACE
                + String.format("%08d", transportAtDestination) + Const.SPACE
                + String.format("%08d", addedValueAtDestination)))
                + Const.ENTER;
    }

    private String formatDate(String dateOfArrivalSMSC) {
        // eventTime = 20140617122524-0300
        // formato fecha salida: 2014/06/17
        String dia, mes, anio;
        anio = dateOfArrivalSMSC.substring(0, 4);
        mes = dateOfArrivalSMSC.substring(4, 6);
        dia = dateOfArrivalSMSC.substring(6, 8);
        return anio + "/" + mes + "/" + dia;
    }

    private String formatTime(String timeOfArrivalSMSC) {
        // eventTime = 20140617122524-0300
        // formato hora salida = 12:25:24
        String hora, min, seg;
        hora = timeOfArrivalSMSC.substring(8, 10);
        min = timeOfArrivalSMSC.substring(10, 12);
        seg = timeOfArrivalSMSC.substring(12, 14);
        return hora + ":" + min + ":" + seg;
    }
}
