# -*- coding: utf-8 -*-
"""
Name: ReportLargeGRSGBurnIncidents
Author: Jennifer McCollom, contractor at BLM NOC, Fire Program
Created On: 01/10/2024 (from a copy of the same script from 2023)
Updated on:

Usage:
Intended to be run within a python idle or through Task Schedular on a local PC... daily during the 2023 calendar year fire season.
Description:
This script runs summary stats on Fire Incident wildfire perimeters in Greater sage-Grouse habitat. Its looking for fires above a certain
acreage threshold... the incidents are listed and emailed to a set of people.
Associations:
Part of the NIFC AGOL Sage Grouse burn reporting for CY2024
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

        #GRSG Burn Threshold and email details
        threshold = 50000 #acres considered a 'large' GRSG burn fires


        #NIFC AGOL to work with data on the site (not downloaded)
        portal = "https://nifc.maps.arcgis.com"
        #User login info
        user = "BLM_SageGrouse"
        bpswd = b'xxxxxx'
        encodedpwdAGOL = base64.b64encode(bpswd)            #Encrypt password (from Danelle)

        e_msg1 = """\
                <html>
                  <head></head>
                  <body>
                    <p>You are receiving this automated email because one or more Wildland Fire Incidents have burned greater than """+"{:,}".format(threshold)+" acres of Greater sage-grouse range. The list of incidents and their associated Greater sage-grouse range acreages burned is below.</p>"

        e_msg2 = """\
                <p>For more information, please visit the 
                    <a href="https://nifc.maps.arcgis.com/apps/dashboards/abdca2762e41406c91aa6076eaf228c8">
                    2025 National Fire Dashboard</a> and click the "Greater Sage-grouse Range" tab below the map.
                </p> 
                <p>Thank you.</p>
                <p>Fire Program Staff<br>
                BLM National Operations Center</p>
                </body></html>"""

        receivers = ["mholloran@blm.gov", "jreese@blm.gov", "cfletcher@blm.gov", "mmclachlan@blm.gov", "djwood@blm.gov","scopeland@blm.gov",
                     "gpoirier@blm.gov","mconry@blm.gov","tjclifford@blm.gov","josterkamp@blm.gov","pdelpizzo@blm.gov","jmccollom@blm.gov",
                     "manthony@blm.gov", "dmmueller@blm.gov","lwaldner@blm.gov" ]
##        receivers = ["jmccollom@blm.gov"]
        
        #GRSG Burn Overlay Dataset 2025:
        itemid_sghabburned = "9dd2284f9f1b4b0eaeda8cebadfdb174"

    except:
        print("!Variables could not be set. Exiting...")
        report_error()
        sys.exit()

    ###-Main Code-###

    try:
        #Connect to NIFC AGOL
        # Connect to AGOL
        print("Connecting to NIFC AGOL")
        nifc_gis = GIS(portal, user, str(base64.b64decode(encodedpwdAGOL),'ascii'))

        # Access INPUT layer
        print("Accessing INPUT Hosted Feature Layer")
        sghabburned_flayer = nifc_gis.content.get(itemid_sghabburned)    #access the item
        sghabburned_layer = sghabburned_flayer.layers[0]                  #grab the first layer of the item
        sghabburned_layer_url = sghabburned_layer.url    
    except:
        print("!Problem connecting to NIFC AGOL and accessing the needed SG Burn dataset.")
        report_error()
        sys.exit()
        
    # Find large fires from today's Greater SG Burn intersect feature class
    print("Running Summary Statistics for GRSG Burn")
    try:
        sumstatstbl = r"memory\sghab_SumStats"
        #Summarize by IRWIN ID to get fire acres
        arcpy.analysis.Statistics(sghabburned_layer_url,sumstatstbl, "GISAcresDBLNG SUM;poly_IncidentName FIRST;poly_CreateDate LAST;NAME FIRST", "irwin_IrwinID")
        #Modify the output table a bit
        arcpy.management.AddField(sumstatstbl,"TotalSGBurnAcres","LONG")
        arcpy.management.CalculateField(sumstatstbl,"TotalSGBurnAcres","!SUM_GISAcresDBLNG!","PYTHON3")
        #Get a View table of just large incidents
        arcpy.management.MakeTableView(sumstatstbl,"LargeSGBurnTableView","TotalSGBurnAcres >= "+str(threshold))
        lrg_fires_cnt = arcpy.management.GetCount("LargeSGBurnTableView")
    except:
        print("!Error... could not summarize data & determine large GRSG burned fires.")
        report_error()
        sys.exit()

    # Get BLM Acres of all of the fires
    print("Running BLM Acre Summary Statistics")
    try:        
        #Isolate BLM only fire acres & summarize
        arcpy.management.MakeFeatureLayer(sghabburned_layer_url,"SG_BLM_burn_only", "AA_Reclass = 'BLM'")

        #Get a list of appropriate Sourec Global IDs (similar to irwin ids) that fit the total GRSG burn criteria (to be able to get the blm acres by those irwinids)       
        irwinid_lst = []
        with arcpy.da.SearchCursor("LargeSGBurnTableView",["irwin_IrwinID"]) as sr_rows:
            for rws in sr_rows:
                irwinid_lst.append(rws[0])
        #Get the total SG applicable BLM acres for the specific irwinid incidents & place in a dictionary
        irwinid_blm_tls_dct = {}
        for irwinid in irwinid_lst:
            tlsacres = 0
            with arcpy.da.SearchCursor("SG_BLM_burn_only",["irwin_IrwinID","GISAcresDBLNG"]) as blmsgrws:
                for irwnrw in blmsgrws:
                    if irwnrw[0] == irwinid:
                        tlsacres = tlsacres + irwnrw[1]
    ##        print("irwinid: "+irwinid+", total blm acres: "+str(tlsacres))
            irwinid_blm_tls_dct[irwinid] = tlsacres
    except:
        print("!Error... could not get BLM acre stats.")
        report_error()
        sys.exit()

    # Write Incidents with GRSG habitat gt threshold acre value to a text string
    print("Writing largest GRSG Burn incident names to a text string.")
    try:
        frstrng = ""
        with arcpy.da.SearchCursor("LargeSGBurnTableView",field_names=["FIRST_poly_IncidentName","TotalSGBurnAcres","irwin_IrwinID","FIRST_NAME","LAST_poly_CreateDate"],sql_clause=(None,"ORDER BY TotalSGBurnAcres DESC")) as rows:
            for row in rows:
                frstrng = frstrng + "<b>{}</b> ({}) - {:,} acres <i>({:,} acres on BLM Land)</i> - Updated: {}<br>".format(row[0],row[3],row[1],irwinid_blm_tls_dct.get(row[2]),row[4].strftime("%m-%d-%Y"))
    except:
        print("Error... could not read large GRSG range burn fires and write the info to a string.")
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
            message['Subject']= "ATTN: Fire(s) detected with large amounts of Greater Sage-grouse range burned (for {})".format(datetime.today().strftime("%m-%d-%Y"))
            mailer=smtplib.SMTP("smtp.blm.gov")
            mailer.send_message(message)
            mailer.quit()
        else:
            print("No incidents found that meet the threshold for GRSG burned areas.")
    except:
        print("Error... emailing did not work.")
        report_error()

    # Cleanup
    print("Cleanup")
    try:
        arcpy.management.Delete(sumstatstbl)
        arcpy.management.Delete("SG_BLM_burn_only")
        arcpy.management.Delete("LargeSGBurnTableView")
    except:
        print("Error... could not do cleanup completely.")
        report_error()
        sys.exit()   

if __name__ == "__main__":
    main()

