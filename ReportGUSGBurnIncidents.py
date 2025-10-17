# -*- coding: utf-8 -*-
"""
Name: ReportGUSGBurnIncidents
Author: Jennifer McCollom, contractor at BLM NOC, Fire Program
Created On: 01/10/2024 (from a copy of the same script from 2023)
Updated on:

Usage:
Intended to be run within a python idle or through Task Schedular on a local PC... daily during the current calendar year fire season.
Description:
This script runs summary stats on Fire Incident wildfire perimeters in Gunnison sage-grouse habitat. Its looking for fires above a certain
acreage threshold... the incidents are listed and emailed to a set of people.
Associations:
Part of the NIFC AGOL Sage Grouse & BLM burn reporting for current calendar year
Note for ifer: {:,}.format(num) with a number in the last part provides output that is comma deliminated at the thousands place
"""
import arcpy, os, urllib, sys, requests, traceback, smtplib, math, base64
from datetime import datetime
from email.message import EmailMessage
from arcgis.gis import GIS

###-Functions-###

def report_error():   
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # Concatenate information together concerning the error into a message string
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"
    # Return python error messages for use in script tool or Python Window
    arcpy.AddMessage(pymsg)
    arcpy.AddMessage(msgs)
    print(pymsg)
    print(msgs)

def main():
    ###-Variables-###
    try:
        # Grab & Format system date & time
        dt = datetime.now()
        datetmstr = dt.strftime("%Y%m%d_%H%M")
        wkday = dt.strftime("%A")

        #GUSG Burn Threshold and email details
        threshold = 50 #acres considered a signifigant GUSG burn fires
    ##    threshold = 1 #acres considered a signifigant GUSG burn fires

        #NIFC AGOL to work with data on the site (not downloaded)
        portal = "https://nifc.maps.arcgis.com"
        #User login info
        user = "BLM_SageGrouse"
        bpswd = b'1hrt%5cRiptG'
        encodedpwdAGOL = base64.b64encode(bpswd)            #Encrypt password (from Danelle)

        e_msg1 = """\
                <html>
                  <head></head>
                  <body>
                    <p>You are receiving this automated email because one or more Wildland Fire Incidents have burned greater than """+"{:,}".format(threshold)+" acres of Gunnison sage-grouse range. The list of incidents and their associated Gunnison sage-grouse range acreages burned is below.</p>"

        e_msg2 = """\
                <p>For more information, please visit the 
                    <a href="https://nifc.maps.arcgis.com/apps/dashboards/abdca2762e41406c91aa6076eaf228c8">
                    2025 National Fire Dashboard</a> and click the "Gunnison Sage-grouse Range" tab below the map.
                </p> 
                <p>Thank you.</p>
                <p>Fire Program Staff<br>
                BLM National Operations Center</p>
                </body></html>"""

##        receivers = ["jmccollom@blm.gov"]
##        receivers = ["josterkamp@blm.gov","lwaldner@blm.gov","pdelpizzo@blm.gov","jmccollom@blm.gov","dmastindixon@blm.gov","cdomschke@blm.gov","manthony@blm.gov","dmmueller@blm.gov"]
        receivers = ["mholloran@blm.gov","jreese@blm.gov", "cfletcher@blm.gov", "mmclachlan@blm.gov","gpoirier@blm.gov","mconry@blm.gov","josterkamp@blm.gov", "dbenhorin@blm.gov", "dpadilla@blm.gov", "jkaminsky@blm.gov" ,"pdelpizzo@blm.gov","jmccollom@blm.gov","dmastindixon@blm.gov","manthony@blm.gov","dmmueller@blm.gov","lwaldner@blm.gov"]
        #GUSG Burn Overlay Dataset 2024:
        itemid_gusghabburned = "fd3d331b6fcd4bf883a8539d55514f46"               #CY2025_BLM_GUSGHabBurned_DashBSprt
        item_id_wfigs_todate_perms = "7c81ab78d8464e5c9771e49b64e834e9"         #WFIGS 2025 Interagency Fire Perimeters to Date

    except:
        print("Variables could not be set. Exiting...")
        report_error()
        sys.exit()

    ###-Main Code-###

    try:
        #Connect to NIFC AGOL
        # Connect to AGOL
        print("Connecting to NIFC AGOL")
        nifc_gis = GIS(portal, user, str(base64.b64decode(encodedpwdAGOL),'ascii'))

        # Access INPUT layers
        print("Accessing INPUT Hosted Feature Layer")
        gusghabburned_flayer = nifc_gis.content.get(itemid_gusghabburned)    #access the item
        gusghabburned_layer = gusghabburned_flayer.layers[0]                  #grab the first layer of the item
        gusghabburned_layer_url = gusghabburned_layer.url

        # Access WFIGS 'todate' perims
        wfigs_perms_todate_flayer = nifc_gis.content.get(item_id_wfigs_todate_perms)    #access the item
        wfigs_perms_todate_layer = wfigs_perms_todate_flayer.layers[0]                  #grab the first layer of the item
        wfigs_td_permslyr_url = wfigs_perms_todate_layer.url                            #grab the full URL of the layer for input with arcpy

    except:
        print("!Problem connecting to NIFC AGOL and accessing the needed SG Burn dataset.")
        report_error()
        sys.exit()

    # Find fires from today's Gunnison SG Burn intersect feature class
    print("Running Summary Statistics for GUSG Burn")
    try:
        sumstatstbl = r"memory\gusghab_SumStats"
        #Summarize by IRWIN ID to get fire acres
        arcpy.analysis.Statistics(gusghabburned_layer_url,sumstatstbl,"GISAcresDBLNG SUM;poly_IncidentName FIRST;poly_CreateDate LAST;NAME FIRST", "attr_SourceGlobalID")
        #Modify the output table a bit
        arcpy.management.AddField(sumstatstbl,"TotalGUSGBurnAcres","LONG")
        arcpy.management.CalculateField(sumstatstbl,"TotalGUSGBurnAcres","!SUM_GISAcresDBLNG!","PYTHON3")
        arcpy.management.JoinField(sumstatstbl,"attr_SourceGlobalID",wfigs_td_permslyr_url,"attr_SourceGlobalID", ["attr_PrimaryFuelModel","attr_SecondaryFuelModel"])
        #Get a selection layer of just large incidents
        arcpy.management.MakeTableView(sumstatstbl,"LargeGUSGBurnTableView","TotalGUSGBurnAcres >= "+str(threshold))
        lrg_fires_cnt = arcpy.management.GetCount("LargeGUSGBurnTableView")
    except:
        print("Error... could not summarize data & determine large GUSG burned fires.")
        report_error()
        sys.exit()


    # Get BLM Acres of all of the fires
    print("Running BLM Acre Summary Statistics & grabbing primary fuel model.")
    try:        
        #Isolate BLM only fire acres & summarize
        arcpy.management.MakeFeatureLayer(gusghabburned_layer_url,"GUSG_BLM_burn_only", "AA_Reclass = 'BLM'")

        #Get a list of appropriate Sourec Global IDs (similar to irwin ids) that fit the total GRSG burn criteria (to be able to get the blm acres by those irwinids)       
        id_lst = []
        with arcpy.da.SearchCursor("LargeGUSGBurnTableView",["attr_SourceGlobalID"]) as sr_rows:
            for rws in sr_rows:
                id_lst.append(rws[0])
        #Get the total SG applicable BLM acres for the specific uniqueid incidents & place in a dictionary
        uniqueid_blm_tls_dct = {}
        uniqueid_blm_pfm_dct = {}
        for uniqueid in id_lst:
            tlsacres = 0
            with arcpy.da.SearchCursor("GUSG_BLM_burn_only",["attr_SourceGlobalID","GISAcresDBLNG"]) as blmsgrws:
                for irwnrw in blmsgrws:
                    if irwnrw[0] == uniqueid:
                        tlsacres = tlsacres + irwnrw[1]
    ##        print("irwinid: "+uniqueid+", total blm acres: "+str(tlsacres))
            uniqueid_blm_tls_dct[uniqueid] = tlsacres
            
            #Get the Primary Fuel Model
            with arcpy.da.SearchCursor("LargeGUSGBurnTableView",["attr_SourceGlobalID","attr_PrimaryFuelModel"]) as lrg_gusg_scurs:
                for incrw in lrg_gusg_scurs:
                    if incrw[0] == uniqueid:
                        pfm_txt = incrw[1]
            uniqueid_blm_pfm_dct[uniqueid] = pfm_txt
    ##        print("Id: "+uniqueid+", Primary Fuel Model: "+uniqueid_blm_pfm_dct.get(uniqueid))
    except:
        print("!Error... could not get BLM acre stats & primary fuel model.")
        report_error()
        sys.exit()

    # Write Incidents with GUSG habitat gt threshold acre value to a text string
    print("Writing GUSG Burn incident names and relavent info to a text string.")
    try:
        frstrng = ""
        with arcpy.da.SearchCursor("LargeGUSGBurnTableView",field_names=["FIRST_poly_IncidentName","TotalGUSGBurnAcres","attr_SourceGlobalID","FIRST_NAME","LAST_poly_CreateDate"],sql_clause=(None,"ORDER BY TotalGUSGBurnAcres DESC")) as rows:
            for row in rows:
                frstrng = frstrng + "<b>{}</b> ({}) - {:,} acres <i>({:,} acres on BLM Land)</i> - Primary Fuel Model: {} - Updated: {}<br>".format(row[0],row[3],row[1],uniqueid_blm_tls_dct.get(row[2]),uniqueid_blm_pfm_dct.get(row[2]),row[4].strftime("%m-%d-%Y"))
    except:
        print("Error... could not read large GUSG range burn fires and write the info to a string.")
        report_error()
        sys.exit()
            
    # Do some fancy email stuff
    print("Sending Email")
    try:
        if int(lrg_fires_cnt[0]) > 0:
            message = EmailMessage()  #creates a 'message' object to work with.
            message.set_content(e_msg1+frstrng+e_msg2,'html')
            message['From']= 'pdelpizzo@blm.gov'
            message['To']= ','.join(receivers)
            message['Subject']= "ATTN: Fire(s) detected with significant amounts of Gunnison Sage-grouse range burned (for {})".format(datetime.today().strftime("%m-%d-%Y"))
            mailer=smtplib.SMTP("smtp.blm.gov")
            mailer.send_message(message)
            mailer.quit()
        else:
            print("No incidents found that meet the threshold for GUSG burned areas.")
    except:
        print("Error... emailing did not work.")
        report_error()

    # Cleanup
    arcpy.AddMessage("Cleanup")
    try:
        arcpy.management.Delete(sumstatstbl)
        arcpy.management.Delete("GUSG_BLM_burn_only")
        arcpy.management.Delete("LargeGUSGBurnTableView")
    except:
        arcpy.AddError("Error... could not do cleanup completely.")
        report_error()
        sys.exit()   

if __name__ == "__main__":
    main()
