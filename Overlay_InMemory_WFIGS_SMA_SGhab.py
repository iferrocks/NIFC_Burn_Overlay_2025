"""
Name: Overlay_InMemory_WFIGS_SMA_SGhab.py
Author: Jennifer McCollom, Contractor at BLM NOC, Fire Program
Date Created: 01/2024 from a copy of the same script ran in 2023
Updated On: 20240409 updated to overlay with SMA published on the BLM Hub GBP on 4/1/2024
Updated On: 20240905 updated to individual check incident size change and send email if an incident growth has exceeded threshold 
Updated On: 20240923 updated to add chunk option to the write_to_AGOL function to avoid an upload error.
Updated On: 20250402 updated to a different way to convert the featureclass into a featureset before uploading to NIFC AGOL
Updated On: 20250402 updated pandas to handle empty data frames and nulls a"Get acres BEFORE analysis" and the change in acres calcluation for the email
Updated On: 20250717 ifer determined that the tally to trigger a GRSG and GUSG email is broken. Bypassed it. Did not fix it today.
Updated On: 20250805 David added section to perfom overlay on Herd Management Areas and publish to agol
Updated On: 20250812 David fixed trigger so it sums acres for all polygons for a single incident for email   
Updated on: 20250917 David Added filter to exclude RX fires from the analysis
Usage:
Intended to be run within a python idle or through Task Schedular on a local or remote PC... daily
(or twice daily) during the fire season.
Description:
This script accesses the NIFC Open Data 'To Date' Wildland Fire Perimeters on AGOL & intersects
the perimeters data with US States, GRSG Habitat, & GUSG Habitat as well as BLM
SDE SMA (at the NOC). The output overwrites existing hosted feature layers on the NIFC's AGOL site.
The attempt in this script is to do as much with online data sources (without copying them locally)
and work in memory to write the output directly to NIFC's AGOL.
"""
import arcpy, arcgis, os, traceback, base64
import pandas as pd
from datetime import datetime
##from arcgis.gis import GIS
from arcgis import GIS

#link to scripts to check for GRSG and GUSG burn
import ReportGUSGBurnIncidents
import ReportLargeGRSGBurnIncidents

#report start time
dt = datetime.now()
datetmstr = dt.strftime("%Y%m%d_%H%M")
print("START TIME: "+datetmstr)

###-VARIABLES-###
try:
    print("Setting variables.")
    
    #NIFC AGOL to work with data on the site (not downloaded)
    portal = "https://nifc.maps.arcgis.com"
    #User login info
    user = "BLM_SageGrouse"
    bpswd = b'1hrt%5cRiptG'
    encodedpwdAGOL = base64.b64encode(bpswd)            #Encrypt password (from Danelle)

    # INPUTS ONLINE:
    #WFIGS CY ToDate Perimeters info:
    item_id_wfigs_todate_perms = "7c81ab78d8464e5c9771e49b64e834e9" #Title 'WFIGS 2025 Interagency Fire Perimeters to Date'

    # INPUT on EGIS/SDE:
    #BLM SMA, GRSG, & GUSG data in the inputs directory
    scrptfolder = os.path.dirname(__file__) #Returns the UNC of the folder where this python file sits
    folder_lst = os.path.split(scrptfolder) #make a list of the head and tail of the scripts folder
    basefolder = folder_lst[0] #Access the head (full directory) where the scripts folder resides
    inputfgdb = os.path.join(basefolder,"Inputs","AlteredSourceData_forOverlayAnalysis.gdb")
    grsg_hab = os.path.join(inputfgdb,"Simplified_GRSGhabitat_202303")
    gusg_hab = os.path.join(inputfgdb,"wildlife_ufws_gusg_habitat_range_2020")
    usstates = os.path.join(inputfgdb,"US_State_Boundaries_2020")
    sma = os.path.join(inputfgdb,"SMA_Simplified_202412_AllUS")
    hma = os.path.join(inputfgdb, "BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons")
    tmpfgdb = r"D:\temp\scratch.gdb"
    outfgdb = r"D:\temp\FY2025_Burn_Overlay_Output.gdb"
    # Overwrite pre-existing files
    arcpy.env.overwriteOutput = True

    # OUTPUTS (CY2025):
    #Log file for tracking when this script is written.             Titled: LastUpdatedLog_BLMSGGrouseBurn_2025
    itemID_log = '61d4289dc7cd4eedb036ac18bb5f72a1'       
    #Natl Burned with only SMA & States (no incident data retained).Titled: CY2025_BLM_SMA_State_NatlBurned_DashBSprt
    item_id_ntl_brn_simple = "33ec3926ed44421ca1cff4dbff1d057b"
    #Natl Burned with SMA of BLMonly, States, & Irwin Incident Info.Titled: CY2025_BLM_NATL_IrwinInfo_DashBSprt
    item_id_ntl_brn_incdtls = "aacee871dc934df0a68aedc9b5502662"
    #GRSG Hab Burned with SMA, States, & Irwin Incident Info.       Titled: CY2025_BLM_SageGrouseHabBurned_DashBSprt
    item_id_grsg_brn_incdtls = "9dd2284f9f1b4b0eaeda8cebadfdb174"
    #GUSG Hab Burned with SMA, States, & Irwin Incident Info        Titled: CY2025_BLM_GUSGHabBurned_DashBSprt
    item_id_gusg_brn_incdtls = "fd3d331b6fcd4bf883a8539d55514f46"
    #Daily tracking table summary of SMA GRSG burn acres            Titled: CY2025_DailyTrackingTbl_SGHabBurn_bySMA_Acres
    item_id_trktbl_sma = "dc2d9a20b7934111889eeef652e739e8"
    #Daily tracking table summary of State GRSG burn acres          Title: CY2025_DailyTrackingTbl_SGHabBurn_byState_Acres
    item_id_trktbl_state = "5775578836d04bb9b8251d2299ff31f7"

    # Log file:
    logfile = os.path.join(basefolder,"NIFC_Burn_Overlays2025_ScriptLog.csv")
    sma_st_msg = "SMA/State overlay not created."
    blm_irwin_msg = "BLM Irwin details overlay not created."
    grsg_msg = "GRSG overlay not created."
    gusg_msg = "GUSG overlay not created."
    log_tbs_msg = "Tracking tables not appended."
    log_df = pd.read_csv(logfile)

    # Thresholds (in acres) for email scripts to alert GRSG and GUSG burns
    grsg_thres = 50000
    gusg_thres = 50

except:
    print("!Problem setting variables.")
    sys.exit()

###-FUNCTIONS-###

#To help report errors in a helpful way. From Karen Robine.
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

# Function to build tally/tracking tables for acreage statistics for this day's download
def buildtally(sourcedata,tallytype,tallyfld):
    try:
        sumstat_tblnm = r"memory\SumStats_"+tallytype
        pivot_tblnm = r"memory\Pivot_"+tallytype
        arcpy.Statistics_analysis(sourcedata,sumstat_tblnm,"GISAcresDB SUM",tallyfld)
        #Add local date/time to a new field
        arcpy.management.AddField(sumstat_tblnm,"pvtfld","TEXT","","",5)
        arcpy.management.CalculateField(sumstat_tblnm,"pvtfld","'temp'","PYTHON3")
        #Pivot sum stats
        arcpy.PivotTable_management(sumstat_tblnm,"pvtfld",tallyfld,"SUM_GISAcresDB",pivot_tblnm)
        return pivot_tblnm
    except:
        report_error()
        return "Errored"

#Write processed data to Portal/AGOL
def write_to_AGOL(layer_nm,mem_nm):
    try:
        cntftrs = arcpy.management.GetCount(mem_nm)
        print('Feature count before upload to NIFC AGOL: '+str(cntftrs))
        if int(cntftrs[0]) > 0:
            #Clear the existing NIFC AGOL item of data
            try:
                layer_nm.manager.truncate()
            except:
                print("Truncate did not work.")
            
            #Convert the data in memory into a format that can be added to the existing hosted feature layer
##            sedf_interim = pd.DataFrame.spatial.from_featureclass(location=mem_nm)
##            print('Feature count of spatial dataframe: '+str(len(sedf_interim)))
##            source_fs = sedf_interim.spatial.to_featureset()
            arcpy_fs = arcpy.FeatureSet(mem_nm)
            source_fs = arcgis.features.FeatureSet.from_arcpy(arcpy_fs)

            try:
                layer_nm.edit_features(adds=source_fs)
                return "arcgis edit feature adds"
            except:

                ftr_cnt = layer_nm.query(return_count_only=True)
                print('Feature count after basic "adds" function to NIFC AGOL: '+str(ftr_cnt))
            

                try:                    #chunk option by jcarlson on esri community
                    i = 0
                    chunk = 1
                    while i < len(sedf_interim):
                        fs = sedf_interim.iloc[i:i+chunk].spatial.to_featureset()
                        layer_nm.edit_features(adds=fs)
                        i += chunk
                    
                    ftr_cnt_chnk = layer_nm.query(return_count_only=True)
                    print('Feature count after chunk "adds" function to NIFC AGOL: '+str(ftr_cnt_chnk))
                    if str(ftr_cnt_chnk) == str(cntftrs):
                        return "arcgis edit feature adds by chunks"
                    else:
                        return "Problem: did not load full features with the 'chunk' option: "+str(ftr_cnt_chnk)+"/"+str(cntftrs)
                
                except Exception as e:
                    report_error()
                    print("!Alternative appending type did not work either.")
                    return "Problem: "+str(e)

                    

        else:
            print("!No records for overlayed dataset found. Not writing data to NIFC AGOL.")
            return "Problem?: No records to append"

        #Clear out the memory file... do not need anymore
##        arcpy.management.Delete(mem_nm)
        
    except:
        print("!Problem writing output to hosted feature layer/table in function.")
        report_error()
        return "No records appended"

# Calculate area in both double and long values and add to dataset
def calc_geod_acr(datast):
    try:
        arcpy.management.AddField(datast,"GISAcresDB","DOUBLE")
        arcpy.management.AddField(datast,"GISAcresDBLNG","LONG")
        arcpy.management.CalculateGeometryAttributes(datast,"GISAcresDB AREA_GEODESIC;GISAcresDBLNG AREA_GEODESIC",'',
                                                     "ACRES",None,"SAME_AS_INPUT")
    except:
        print("!Problem adding and calculating attributes for geodesic area.")
        report_error()

# Calculate the sum of a number field in a dataset and returns the value.
def sum_nmb_field(dtst,fld,sqlcls):
    try:
        ttlnum = 0
        scurs = arcpy.da.SearchCursor(dtst,[fld],sqlcls)
        for srow in scurs:
            ttlnum = ttlnum + srow[0]
        return ttlnum
    except:
        print("!Problem summing the field requested.")
        report_error()
        return 0
def rename_fields_for_agol(mem_nm):
    rename_map = {
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HMA_NAME': 'HMA_NAME',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HMA_ID': 'HMA_ID',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_ADMIN_ST': 'ADMIN_ST',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_ADM_UNIT_C': 'ADM_UNIT_C',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_ADMIN_AGCY': 'ADMIN_AGCY',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HERD_TYPE': 'HERD_TYPE',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_OBJECTID_1': 'OBJECTID_1',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HMA_NM': 'HMA_NM',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_ADMIN_ST_1': 'ADMIN_ST_1',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HMA_IDENTI': 'HMA_IDENTI',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_TRANSFER_A': 'TRANSFER_A',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_BLM_ACRES': 'BLM_ACRES',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_TOTAL_ACRE': 'TOTAL_ACRE',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HORSE_AML_': 'HORSE_AML_',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_HORSE_AML1': 'HORSE_AML1',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_EST_HORSE_': 'EST_HORSE_',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_PERCENT_AM': 'PERCENT_AM',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_BURRO_AML_': 'BURRO_AML_',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_BURRO_AML1': 'BURRO_AML1',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_EST_BURRO_': 'EST_BURRO_',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_POP_INVENT': 'POP_INVENT',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_LAST_GATHE': 'LAST_GATHE',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_MOST_RECEN': 'MOST_RECEN',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_MESSAGE': 'MESSAGE',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_WEBSITE_UR': 'WEBSITE_UR',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_PERCENT__1': 'PERCENT_1',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_PERCENT__2': 'PERCENT_2',
        'BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_PERCENT__3': 'PERCENT_3',
        'HMA_Burn_Summary_SUM_SUM_BurnedAcres': 'SUM_BurnedAcres',
        'SHAPE_Length': 'Shape__Length',
        'SHAPE_Area': 'Shape__Area'
    }
    for old_name, new_name in rename_map.items():
        try:
            arcpy.management.AlterField(mem_nm, old_name, new_name, new_name)
            print(f"Renamed {old_name} → {new_name}")
        except Exception as e:
            print(f"Failed to rename {old_name}: {e}")
###-MAIN-###
# Connect to AGOL and grab the layers needed
try:
    # Connect to AGOL
    print("Connecting to NIFC AGOL")
    nifc_gis = GIS(portal, user, str(base64.b64decode(encodedpwdAGOL),'ascii'),expiration=9999)

    # Access INPUT layers & their urls
    print("Accessing INPUT Hosted Feature Layers & related objects")
    #todate perims
    wfigs_perms_todate_flayer = nifc_gis.content.get(item_id_wfigs_todate_perms)    #access the item
    wfigs_perms_todate_layer = wfigs_perms_todate_flayer.layers[0]                  #grab the first layer of the item
    wfigs_td_permslyr_url = wfigs_perms_todate_layer.url                            #grab the full URL of the layer for input with arcpy
    
    # Access OUTPUT Hosted layers
    print("Accessing OUTPUT Hosted Feature Layers")
    #Log file
    htable_log = nifc_gis.content.get(itemID_log)
    edit_htab_log = htable_log.tables[0]
    #Natl Burned with only SMA & States (no incident data retained)
    ntl_brn_simple_flayer = nifc_gis.content.get(item_id_ntl_brn_simple)            #access the item
    ntl_brn_simple_layer = ntl_brn_simple_flayer.layers[0]                          #grab the first layer of the item
    #Natl Burned with SMA of BLMonly, States, & Irwin Incident Info
    ntl_brn_incdtls_flayer = nifc_gis.content.get(item_id_ntl_brn_incdtls)
    ntl_brn_incdtls_layer = ntl_brn_incdtls_flayer.layers[0]
    #GRSG Hab Burned with SMA, States, & Irwin Incident Info
    grsg_brn_incdtls_flayer = nifc_gis.content.get(item_id_grsg_brn_incdtls)
    grsg_brn_incdtls_layer = grsg_brn_incdtls_flayer.layers[0]
    #GUSG Hab Burned with SMA, States, & Irwin Incident Info
    gusg_brn_incdtls_flayer = nifc_gis.content.get(item_id_gusg_brn_incdtls)
    gusg_brn_incdtls_layer = gusg_brn_incdtls_flayer.layers[0]

    # Access OUTPUT Hosted tables
    #Daily tracking table summary of SMA GRSA burn acres
    trktbl_sma_ftable = nifc_gis.content.get(item_id_trktbl_sma)
    trktbl_sma_table = trktbl_sma_ftable.tables[0]
    #Daily tracking table summary of State GRSA burn acres
    trktbl_state_ftable = nifc_gis.content.get(item_id_trktbl_state)
    trktbl_state_table = trktbl_state_ftable.tables[0]
    
except:
    print("!Problem connecting to the NIFC AGOL and accessing needed layers.")
    report_error()
    sys.exit()

### Run the overlay functions with the AGOL item layers as inputs (without copying to local PC)
try:

    ### Get acres for GRSG BEFORE analysis ###
    obj_cols = ['GISAcresDB']
    df_grsgacres_bf = pd.DataFrame.spatial.from_layer(grsg_brn_incdtls_layer)
    df_grsgacres_bf_grouped = (df_grsgacres_bf.groupby(['irwin_IncidentName'], as_index=False).agg({'GISAcresDB': 'sum'}))
    df_grsgacres_bf_filt = df_grsgacres_bf_grouped.loc[(df_grsgacres_bf_grouped['GISAcresDB'] > grsg_thres)].copy()
    # Handle empty DataFrame by assigning default zero value
    if df_grsgacres_bf_filt.empty:
        df_grsgacres_bf_filt = pd.DataFrame([{'GISAcresDB': 0, 'irwin_IncidentName': 'N/A'}])
    df_grsgacres_bf_filt[obj_cols] *= -1
    # Get acres for GUSG BEFORE analysis
    df_gusgacres_bf = pd.DataFrame.spatial.from_layer(gusg_brn_incdtls_layer)
    df_gusgacres_bf_grouped = (df_gusgacres_bf.groupby(['irwin_IncidentName'], as_index=False).agg({'GISAcresDB': 'sum'}))
    df_gusgacres_bf_filt = df_gusgacres_bf_grouped.loc[(df_gusgacres_bf_grouped['GISAcresDB'] > gusg_thres)].copy()
    # Handle empty DataFrame by assigning default zero value
    if df_gusgacres_bf_filt.empty:
        df_gusgacres_bf_filt = pd.DataFrame([{'GISAcresDB': 0, 'irwin_IncidentName': 'N/A'}])
    df_gusgacres_bf_filt[obj_cols] *= -1
    print("Acres of GRSG Prev:")
    print(df_grsgacres_bf_filt[['GISAcresDB', 'irwin_IncidentName']])
    print("\nAcres of GUSG Prev:")
    print(df_gusgacres_bf_filt[['GISAcresDB', 'irwin_IncidentName']])
    print("Starting overlay of WFIGS ToDate Perimeters.")
    # #shorthand memory for outputs
    # print("Datasets will be stored in Memory.")
    # wfigs_td_copy = r"memory\wfigs_td_perims"
    # natlsts_outds = r"memory\overlay_state"
    # natlsts_sma_outds = r"memory\overlay_sma"
    # natlblmdts_outds = r"in_memory\diss_ntlblmdts"   #this one is used for most of the outputs in some format
    # natlblm_outds = r"in_memory\diss_ntlblm"
    # grsg_ovly_outds = r"memory\overlay_grsg"
    # grsg_diss_outds = r"in_memory\diss_grsg"
    # gusg_ovly_outds = r"memory\overlay_gusg"
    # gusg_diss_outds = r"in_memory\diss_gusg"
    # deletemem = 'y'
    
    #local harddrive shorthand for outputs
    print("Datasets will be stored on harddrive.")
    wfigs_td_temp = os.path.join(tmpfgdb,"wfigs_td_perims_temp")
    wfigs_td_copy = os.path.join(tmpfgdb,"wfigs_td_perims")
    natlsts_outds = os.path.join(tmpfgdb,"overlay_state")
    natlsts_sma_outds = os.path.join(tmpfgdb,"overlay_sma")
    #natlblmdts_outds = os.path.join(tmpfgdb,"diss_ntlblmdts")   #this one is used for most of the outputs in some format
    natlblmdts_outds = os.path.join(outfgdb,"CY2024_BLM_NATL_IrwinInfo_DashBSprt")   #this one is used for most of the outputs in some format
    natlblm_outds = os.path.join(tmpfgdb,"diss_ntlblm")
    grsg_ovly_outds = os.path.join(tmpfgdb,"overlay_grsg")
    grsg_diss_outds = os.path.join(tmpfgdb,"diss_grsg")
    gusg_ovly_outds = os.path.join(tmpfgdb,"overlay_gusg")
    gusg_diss_outds = os.path.join(tmpfgdb,"diss_gusg")
    deletemem = ''        #uncommenting this with this code block keeps the most important dataset from being erased at the end of the script in order to be manually examined.
    
    #overlaying with us states
    print(".....identify with US States.")
    try:
        print("Using the WFIGS URL.")
        arcpy.analysis.Identity(wfigs_td_permslyr_url, usstates, natlsts_outds)
    except:
        try:
            print("Error with Identity using URL. Attempting to convert WFIGS data to a feature class.")
            arcpy.conversion.ExportFeatures(wfigs_td_permslyr_url, wfigs_td_copy)
            arcpy.analysis.Identity(wfigs_td_copy, usstates, natlsts_outds)
        except:
            try:
                print("Error with WFIGS URL. Attempting again to convert WFIGS data to a feature class.")
                wfigs_tdperims_sdf = pd.DataFrame.spatial.from_layer(wfigs_perms_todate_layer)
                wfigs_tdperims_sdf.spatial.to_featureclass(location=wfigs_td_copy)
                arcpy.analysis.Identity(wfigs_td_copy, usstates, natlsts_outds)
            except:
                print("!Problem with accessing the WFIGS source data. Exiting.")
                report_error()
                sys.exit()
    rx_field = "attr_IncidentTypeCategory"
    fields = [f.name for f in arcpy.ListFields(natlsts_outds)]
    if rx_field in fields:
        arcpy.management.MakeFeatureLayer(natlsts_outds, "wfigs_filtered", f"{rx_field} <> 'RX'")
        arcpy.management.CopyFeatures("wfigs_filtered", wfigs_td_copy)
        print("RX fire perimeters excluded.")
    else:
        print(f"RX exclusion skipped")
        arcpy.management.CopyFeatures(natlsts_outds, wfigs_td_copy)
    
    #overlaying with SMA
    print(".....intersecting with SMA.")
    arcpy.analysis.Intersect([[natlsts_outds,1],[sma,2]],natlsts_sma_outds)
    arcpy.management.Delete(natlsts_outds)

    # Natl (BLM only) Burn Intersect Outputs
    print(".....creating output of Natl BLM only datasets with & without IRWIN Inc Details.")
    arcpy.management.MakeFeatureLayer(natlsts_sma_outds,"blmonly","AA_Reclass = 'BLM'")

    #--Natl BLM only Burn simple--
    print(".....dissolving Natl BLM burn simple overlay.")
    arcpy.analysis.PairwiseDissolve("blmonly",natlblm_outds,["NAME","AA_Reclass"],None,"MULTI_PART")
    arcpy.management.JoinField(natlblm_outds,"NAME",usstates, "NAME", "STATE_ABBR")
    #run function to calculate geodesic acres in both double and long format
    calc_geod_acr(natlblm_outds)

    # Write to AGOL with function: Natl BLM only Burn simple
    print("..writing to: CY2024_BLM_SMA_State_NatlBurned_DashBSprt")
    sma_st_msg = write_to_AGOL(ntl_brn_simple_layer,natlblm_outds)

    #arcpy.management.CopyFeatures("blmonly",os.path.join(tmpfgdb,"blmonly"))

    #--Natl BLM only Burn with IRWIN Deets--
    print(".....dissolving Natl BLM burn details overlay.")
    arcpy.analysis.PairwiseDissolve("blmonly",natlblmdts_outds,["attr_SourceGlobalID","NAME","AA_Reclass"],None,"MULTI_PART")
    arcpy.management.JoinField(natlblmdts_outds,"attr_SourceGlobalID",wfigs_td_copy,"attr_SourceGlobalID",
                               ['attr_IrwinID','attr_FORID','attr_IncidentName','attr_ICS209ReportStatus','attr_POOState'])
    arcpy.management.JoinField(natlblmdts_outds,"NAME",usstates, "NAME", "STATE_ABBR")
    #manage field names after 3/3/2023 update of WFIGS services to older names in our NIFC AGOL item
    arcpy.management.AddField(natlblmdts_outds,"irwin_IrwinID","TEXT",field_length=38)
    arcpy.management.AddField(natlblmdts_outds,"irwin_IncidentName","TEXT",field_length=50)
    arcpy.management.AddField(natlblmdts_outds,"irwin_ICS209ReportStatus","TEXT",field_length=1)
    arcpy.management.AddField(natlblmdts_outds,"irwin_POOState","TEXT",field_length=6)
    #calculate the new fields with the proper attribute field mapping when writing to NIFC AGOL
    arcpy.management.CalculateField(natlblmdts_outds,"irwin_IrwinID","!attr_IrwinID!","PYTHON3")
    arcpy.management.CalculateField(natlblmdts_outds,"irwin_IncidentName","!attr_IncidentName!","PYTHON3")
    arcpy.management.CalculateField(natlblmdts_outds,"irwin_ICS209ReportStatus","!attr_ICS209ReportStatus!","PYTHON3")
    arcpy.management.CalculateField(natlblmdts_outds,"irwin_POOState","!attr_POOState!","PYTHON3")

    #run function to calculate geodesic acres in both double and long format
    calc_geod_acr(natlblmdts_outds)
    #populate 'irwin_ics209ReportStatus' values for nulls to 'N'
    arcpy.management.MakeFeatureLayer(natlblmdts_outds,"status_blanks","irwin_ICS209ReportStatus IS NULL", None)
    arcpy.management.CalculateField("status_blanks","irwin_ICS209ReportStatus","'N'","PYTHON3")
    #populate 'irwin_irwinID' with blanks (because they are sourced from FODR) with the FODR ID
    arcpy.management.MakeFeatureLayer(natlblmdts_outds,"irwinID_blanks","irwin_IrwinID IS NULL", None)
    arcpy.management.CalculateField("irwinID_blanks","irwin_IrwinID","!attr_FORID!","PYTHON3")

##    arcpy.management.CopyFeatures(natlblmdts_outds,os.path.join(tmpfgdb,"natlblmdts_outds_20231012"))

    #Try cleaning up the data a bit...
    print("..running Repair Geometry 3 times")
    arcpy.management.RepairGeometry(natlblmdts_outds, "DELETE_NULL", "ESRI")
    arcpy.management.RepairGeometry(natlblmdts_outds, "DELETE_NULL", "OGC")
    arcpy.management.RepairGeometry(natlblmdts_outds, "DELETE_NULL", "ESRI")

    # Write to AGOL with function: Natl BLM only Burn with IRWIN Deets
    print("..writing to: CY2024_BLM_NATL_IrwinInfo_DashBSprt")
    blm_irwin_msg = write_to_AGOL(ntl_brn_incdtls_layer,natlblmdts_outds)


    #delete the blm only sma feature layer   
    arcpy.management.Delete("blmonly")
    arcpy.management.Delete("status_blanks")
    arcpy.management.Delete("irwinID_blanks")
            

    # Sage Grouse Burn Intersect Outputs
    print(".....creating output of SG habitat with WFIGS perimeters, State info, & SMA.")

    #--GRSG--
    # clip to GRSG Habitat shapes - using clip because no GRSG habitat fields need to be retained
    print(".....overlay with GRSG Habitat")
    arcpy.analysis.Intersect([[natlsts_sma_outds,1],[grsg_hab,2]], grsg_ovly_outds,"NO_FID")

    # simplify output & get only the attribute fields needed
    arcpy.analysis.PairwiseDissolve(grsg_ovly_outds,grsg_diss_outds,"attr_SourceGlobalID;NAME;AA_Reclass",None,"MULTI_PART")
    arcpy.management.Delete(grsg_ovly_outds)
    arcpy.management.JoinField(grsg_diss_outds,"attr_SourceGlobalID",wfigs_td_copy,"attr_SourceGlobalID",
                               ["attr_IrwinID","attr_FORID","attr_IncidentName","attr_ICS209ReportStatus","attr_POOState",
                                "attr_ModifiedOnDateTime_dt","attr_CreatedOnDateTime_dt",
                                "poly_IncidentName","poly_CreateDate","poly_SourceGlobalID","poly_Source"])
    arcpy.management.JoinField(grsg_diss_outds,"NAME",usstates, "NAME", "STATE_ABBR")
    #manage field names after 3/3/2023 update of WFIGS services to older names
    arcpy.management.AddField(grsg_diss_outds,"irwin_IrwinID","TEXT",field_length=38)
    arcpy.management.AddField(grsg_diss_outds,"irwin_IncidentName","TEXT",field_length=50)
    arcpy.management.AddField(grsg_diss_outds,"irwin_ICS209ReportStatus","TEXT",field_length=1)
    arcpy.management.AddField(grsg_diss_outds,"irwin_POOState","TEXT",field_length=6)
    arcpy.management.AddField(grsg_diss_outds,"irwin_ModifiedOnDateTime_dt","DATE")
    arcpy.management.AddField(grsg_diss_outds,"irwin_CreatedOnDateTime_dt","DATE")
    arcpy.management.AddField(grsg_diss_outds,"poly_GlobalID","TEXT",field_length=38)
    #calculate the new fields with the proper attribute field mapping when writing to NIFC AGOL
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_IrwinID","!attr_IrwinID!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_IncidentName","!attr_IncidentName!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_ICS209ReportStatus","!attr_ICS209ReportStatus!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_POOState","!attr_POOState!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_ModifiedOnDateTime_dt","!attr_ModifiedOnDateTime_dt!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"irwin_CreatedOnDateTime_dt","!attr_CreatedOnDateTime_dt!","PYTHON3")
    arcpy.management.CalculateField(grsg_diss_outds,"poly_GlobalID","!poly_SourceGlobalID!","PYTHON3")
    #run function to calculate geodesic acres in both double and long format
    calc_geod_acr(grsg_diss_outds)
    #populate 'irwin_ics209ReportStatus' values for nulls to 'N'
    arcpy.management.MakeFeatureLayer(grsg_diss_outds,"status_blanks","irwin_ICS209ReportStatus IS NULL", None)
    arcpy.management.CalculateField("status_blanks","irwin_ICS209ReportStatus","'N'","PYTHON3")
    #populate 'irwin_irwinID' with blanks (because they are sourced from FODR) with the FODR ID
    arcpy.management.MakeFeatureLayer(grsg_diss_outds,"irwinID_blanks","irwin_IrwinID IS NULL", None)
    arcpy.management.CalculateField("irwinID_blanks","irwin_IrwinID","!attr_FORID!","PYTHON3")
##        arcpy.management.DeleteField(grsg_diss_outds,["attr_SourceGlobalID"])

    # Get the tally table info calculated and appended for SG Habitat burned by SMA agency & US State
    sma_pvt_tbl = buildtally(grsg_diss_outds,"SMA_Tally","AA_Reclass")
    state_pvt_tbl = buildtally(grsg_diss_outds,"States_Tally","NAME")

    # Write to AGOL with function: GRSG Burned areas by state and SMA
    print("..writing to: CY2024_BLM_SageGrouseHabBurned_DashBSprt")
    grsg_msg = write_to_AGOL(grsg_brn_incdtls_layer,grsg_diss_outds)
    arcpy.management.Delete("status_blanks")
    arcpy.management.Delete("irwinID_blanks")

    #--GUSG--
    # intersect with GUSG Habitat shapes
    print(".....intersecting with GUSG Habitat.")
    arcpy.analysis.Intersect([[natlsts_sma_outds,1],[gusg_hab,2]],gusg_ovly_outds)

    # simplify output & get only the attribute fields needed
    arcpy.analysis.PairwiseDissolve(gusg_ovly_outds,gusg_diss_outds,["attr_SourceGlobalID","NAME","AA_Reclass","Population","Status_Rcls"],
                                    None,"MULTI_PART")
    arcpy.management.Delete(gusg_ovly_outds)
    arcpy.management.JoinField(gusg_diss_outds,"attr_SourceGlobalID",wfigs_td_copy,"attr_SourceGlobalID",
                               ["attr_IrwinID","attr_FORID","attr_IncidentName","attr_ICS209ReportStatus","attr_POOState",
                                "attr_ModifiedOnDateTime_dt","attr_CreatedOnDateTime_dt",
                                "poly_IncidentName","poly_CreateDate","poly_SourceGlobalID","poly_Source"])
    arcpy.management.JoinField(gusg_diss_outds,"NAME",usstates, "NAME", "STATE_ABBR")
    #manage field names after 3/3/2023 update of WFIGS services to older names
    arcpy.management.AddField(gusg_diss_outds,"irwin_IrwinID","TEXT",field_length=38)
    arcpy.management.AddField(gusg_diss_outds,"irwin_IncidentName","TEXT",field_length=50)
    arcpy.management.AddField(gusg_diss_outds,"irwin_ICS209ReportStatus","TEXT",field_length=1)
    arcpy.management.AddField(gusg_diss_outds,"irwin_POOState","TEXT",field_length=6)
    arcpy.management.AddField(gusg_diss_outds,"irwin_ModifiedOnDateTime_dt","DATE")
    arcpy.management.AddField(gusg_diss_outds,"irwin_CreatedOnDateTime_dt","DATE")
    arcpy.management.AddField(gusg_diss_outds,"poly_GlobalID","TEXT",field_length=38)
    #calculate the new fields with the proper attribute field mapping when writing to NIFC AGOL
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_IrwinID","!attr_IrwinID!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_IncidentName","!attr_IncidentName!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_ICS209ReportStatus","!attr_ICS209ReportStatus!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_POOState","!attr_POOState!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_ModifiedOnDateTime_dt","!attr_ModifiedOnDateTime_dt!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"irwin_CreatedOnDateTime_dt","!attr_CreatedOnDateTime_dt!","PYTHON3")
    arcpy.management.CalculateField(gusg_diss_outds,"poly_GlobalID","!poly_SourceGlobalID!","PYTHON3")
    #run function to calculate geodesic acres in both double and long format
    calc_geod_acr(gusg_diss_outds)
    #populate 'irwin_ics209ReportStatus' values for nulls to 'N'
    arcpy.management.MakeFeatureLayer(gusg_diss_outds,"status_blanks","irwin_ICS209ReportStatus IS NULL", None)
    arcpy.management.CalculateField("status_blanks","irwin_ICS209ReportStatus","'N'","PYTHON3")
    #populate 'irwin_irwinID' with blanks (because they are sourced from FODR) with the FODR ID
    arcpy.management.MakeFeatureLayer(gusg_diss_outds,"irwinID_blanks","irwin_IrwinID IS NULL", None)
    arcpy.management.CalculateField("irwinID_blanks","irwin_IrwinID","!attr_FORID!","PYTHON3")
##        arcpy.management.DeleteField(gusg_diss_outds,["attr_SourceGlobalID"])

    # Write to AGOL with function: GUSG Burned areas by state and SMA
    print("..writing to: CY2024_BLM_GUSGHabBurned_DashBSprt")
    gusg_msg = write_to_AGOL(gusg_brn_incdtls_layer,gusg_diss_outds)
    arcpy.management.Delete("status_blanks")
    arcpy.management.Delete("irwinID_blanks")
    # Append the data from the tally tables of sage grouse sma and state burned
    print("Appending GRSG burned information for states and SMA agencies to AGOL tally tables.")  
    #read the results of the tally pivot tables into a dictionary
    pvtupdate_dict = {sma_pvt_tbl:trktbl_sma_table,state_pvt_tbl:trktbl_state_table}
    for pvttbl,nifcagoltbl in pvtupdate_dict.items():
        loaddict = {}
        fld_nmlst = [f.name for f in arcpy.ListFields(pvttbl)]
        fld_nmlst.remove('OBJECTID')
        fld_nmlst.remove('pvtfld')
        if len(fld_nmlst) > 0:
            with arcpy.da.SearchCursor(pvttbl,fld_nmlst) as scurs:
                for row in scurs:
                    indxn = 0
                    for nm in fld_nmlst:
                        loaddict[nm] = row[indxn]
                        indxn = indxn+1
            loaddict['Download_Date_UTC'] = dt
            add_rcrd = {"attributes":loaddict}
            nifcagoltbl.edit_features(adds=[add_rcrd])
        else:
            print("!No records to append to the online tally table: "+nifcagoltbl.properties.name)
        arcpy.management.Delete(pvttbl)
    log_tbs_msg = "Appended"

    # --- GRSG After Analysis ---
    print("Calculating GRSG")
    df_grsgacres = pd.DataFrame.spatial.from_featureclass(grsg_diss_outds)
    df_grsgacres_grouped = (df_grsgacres.groupby(['irwin_IncidentName'], as_index=False).agg({'GISAcresDB': 'sum'}))
    df_grsgacres_filt = df_grsgacres_grouped.loc[df_grsgacres_grouped['GISAcresDB'] > grsg_thres].copy()
    # Print all incidents intersecting GRSG habitat above threshold
    if df_grsgacres_filt.empty:
        df_grsgacres_filt = pd.DataFrame([{
            'GISAcresDB': 0,
            'irwin_IncidentName': 'N/A',
        }])
    if df_grsgacres_bf_filt.empty:
        df_grsgacres_bf_filt = pd.DataFrame([{
            'GISAcresDB': 0,
            'irwin_IncidentName': 'N/A',
        }])
    df_grsgacres_filt = df_grsgacres_filt.rename(columns={'GISAcresDB': 'Acres_Current'})
    df_grsgacres_bf_filt = df_grsgacres_bf_filt.rename(columns={'GISAcresDB': 'Acres_Before'})
    print("GRSG Acres Before:")
    print(df_grsgacres_bf_filt[['irwin_IncidentName', 'Acres_Before']])
    print("GRSG Acres Current:")
    print(df_grsgacres_filt[['irwin_IncidentName', 'Acres_Current']])
    df_grsgacres_diff = pd.merge(
        df_grsgacres_filt[['irwin_IncidentName', 'Acres_Current']],
        df_grsgacres_bf_filt[['irwin_IncidentName', 'Acres_Before']],
        on=['irwin_IncidentName'],
        how='outer'
    )
    df_grsgacres_diff.fillna(0, inplace=True)
    df_grsgacres_diff['Acres_Diff'] = df_grsgacres_diff['Acres_Current'] + df_grsgacres_diff['Acres_Before']
    print("Difference in GRSG is:")
    print(df_grsgacres_diff[['irwin_IncidentName', 'Acres_Diff']])
    # --- GUSG After Analysis ---
    try:
        print("Calculating GUSG")
        df_gusgacres = pd.DataFrame.spatial.from_featureclass(gusg_diss_outds)
        df_gusgacres_grouped = (df_gusgacres.groupby(['irwin_IncidentName'], as_index=False).agg({'GISAcresDB': 'sum'}))
        df_gusgacres_filt = df_gusgacres_grouped.loc[df_gusgacres_grouped['GISAcresDB'] > gusg_thres].copy()
        if df_gusgacres_filt.empty:
            df_gusgacres_filt = pd.DataFrame([{
                'GISAcresDB': 0,
                'irwin_IncidentName': 'N/A',
            }])
        if df_gusgacres_bf_filt.empty:
            df_gusgacres_bf_filt = pd.DataFrame([{
                'GISAcresDB': 0,
                'irwin_IncidentName': 'N/A',
            }])
        df_gusgacres_filt = df_gusgacres_filt.rename(columns={'GISAcresDB': 'Acres_Current'})
        df_gusgacres_bf_filt = df_gusgacres_bf_filt.rename(columns={'GISAcresDB': 'Acres_Before'})
        print("GUSG Acres Before:")
        print(df_gusgacres_bf_filt[['irwin_IncidentName', 'Acres_Before']])
        print("GUSG Acres Current:")
        print(df_gusgacres_filt[['irwin_IncidentName', 'Acres_Current']])
        df_gusgacres_diff = pd.merge(
            df_gusgacres_filt[['irwin_IncidentName', 'Acres_Current']],
            df_gusgacres_bf_filt[['irwin_IncidentName', 'Acres_Before']],
            on=['irwin_IncidentName'],
            how='outer'
        )
        df_gusgacres_diff.fillna(0, inplace=True)
        df_gusgacres_diff['Acres_Diff'] = df_gusgacres_diff['Acres_Current'] + df_gusgacres_diff['Acres_Before']

        print("Difference in GUSG is:")
        print(df_gusgacres_diff[['irwin_IncidentName', 'Acres_Diff']])
        if (df_grsgacres_diff['Acres_Diff'] > 100).any():
            print("Running script to look for large GRSG hab burn fires & possibly sending an email.")
            ReportLargeGRSGBurnIncidents.main()
        if (df_gusgacres_diff['Acres_Diff'] > 5).any():
            print("Running script to look for GUSG hab burn fires & possibly sending an email.")
            ReportGUSGBurnIncidents.main()
    except Exception as e:
        print(f"Getting the GUSG difference is not working: {e}")
    # --- Cleanup ---
    print(".....intersecting HMA with SMA.")
    if deletemem == 'y':
        arcpy.management.Delete(natlblm_outds)
        arcpy.management.Delete(natlblmdts_outds)
        arcpy.management.Delete(grsg_diss_outds)
        arcpy.management.Delete(gusg_diss_outds)
### Overlay analysis and push for Herd Management Areas ####
    # Intersect HMA, SMA, and fire perimeters
    hma_burn_outds = os.path.join(tmpfgdb,"overlay_hma_sma_burn")
    hma_dissolved_outds = os.path.join(tmpfgdb, "HMA_Burn_Areas")  # First dissolve output
    HMAInc_dissolved = os.path.join(tmpfgdb, "HMA_Burn_Summary")   # Final summary per HMA
    hma_join_outds = os.path.join(tmpfgdb, "HMA_Areas")            # Final joined output
    arcpy.analysis.Intersect([[hma,1],[sma,2],[wfigs_td_copy,3]], hma_burn_outds)
    print("Intersected output path:", hma_burn_outds)
    if not arcpy.Exists(hma_burn_outds):
        raise RuntimeError("Intersect output not created: check input paths/layers.")
    if "BurnedAcres" not in [f.name for f in arcpy.ListFields(hma_burn_outds)]:
        arcpy.management.AddField(hma_burn_outds, "BurnedAcres", "DOUBLE")
    arcpy.management.CalculateGeometryAttributes(hma_burn_outds, [["BurnedAcres", "AREA_GEODESIC"]], area_unit="ACRES")
    #Clean fields
    fields_to_keep = [
        "OBJECTID", "HMA_NAME", "HMA_ID", "ADMIN_ST", "ADM_UNIT_CD", "ADMIN_AGCY", "HERD_TYPE",
        "HMA_NM", "ADMIN_ST_1", "HMA_IDENTIFIER", "attr_UniqueFireIdentifier",
        "attr_POOJurisdictionalAgency", "attr_POOState", "AA_Reclass", "BurnedAcres"
    ]
    field_names = [f.name for f in arcpy.ListFields(hma_burn_outds)
                   if f.name not in fields_to_keep and not f.required]
    if field_names:
        arcpy.management.DeleteField(hma_burn_outds, field_names)
    #Dissolve by jurisdiction + fire + HMA
    dissolve_fields = [
        "HMA_NAME", "HMA_ID", "ADMIN_ST", "ADM_UNIT_CD", "ADMIN_AGCY", "HERD_TYPE",
        "HMA_NM", "ADMIN_ST_1", "HMA_IDENTIFIER", "attr_POOJurisdictionalAgency",
        "attr_POOState", "AA_Reclass", "attr_UniqueFireIdentifier"
    ]
    dissolve_stats = [["BurnedAcres", "SUM"]]
    arcpy.management.Dissolve(
        in_features=hma_burn_outds,
        out_feature_class=hma_dissolved_outds,
        dissolve_field=dissolve_fields,
        statistics_fields=dissolve_stats,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES")
    #Dissolve again to summarize total burned acres per HMA
    arcpy.management.Dissolve(
        in_features=hma_dissolved_outds,
        out_feature_class=HMAInc_dissolved,
        dissolve_field=["HMA_ID","AA_Reclass"],
        statistics_fields=[["SUM_BurnedAcres", "SUM"]],
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES"
    )
    arcpy.management.MakeFeatureLayer(hma, "hma_layer")
    arcpy.management.AddJoin(
        in_layer_or_view="hma_layer",
        in_field="HMA_ID",
        join_table=HMAInc_dissolved,
        join_field="HMA_ID"
    )
    where_clause = '"HMA_Burn_Summary.SUM_SUM_BurnedAcres" IS NOT NULL'
    arcpy.management.SelectLayerByAttribute("hma_layer", "NEW_SELECTION", where_clause)
    arcpy.management.CopyFeatures("hma_layer", hma_join_outds)
    def strip_join_prefix(fc, prefix):
        fields = arcpy.ListFields(fc)
        for field in fields:
            if field.name.startswith(prefix) and not field.required:
                new_name = field.name.replace(prefix, "")
                try:
                    arcpy.management.AlterField(fc, field.name, new_name, new_name)
                except Exception as e:
                    print(f"Could not fix {field.name}: {e}")

    # Apply to joined output
    arcpy.management.AlterField(hma_join_outds,"HMA_Burn_Summary_SUM_SUM_BurnedAcres","HMA_Burn_Summary_SUM_SUM_Burned", "SUM_SUM_BurnedAcres")
    join_prefix = "BLM_Natl_Wild_Horse_and_Burro_Herd_Mgmt_Area_Polygons_"
    strip_join_prefix(hma_join_outds, join_prefix)
    arcpy.management.RemoveJoin("hma_layer")
    # --- Publish Burned Overlay Features ---
    print("..publishing HMA Burned Overlay Features")
    hma_burned_item_id = "0a290b1c2013430d898c40f41e787896"
    hma_overlay_flayer = nifc_gis.content.get(hma_burned_item_id)
    hma_overlay_layer = hma_overlay_flayer.layers[0]
    write_to_AGOL(hma_overlay_layer, hma_dissolved_outds)
    # --- Publish Dissolved Burn Summary Features ---
    print("..publishing HMA Dissolved Burn Summary Features")
    hma_summary_item_id = "8633171ca45d4626bce388ff4150be7d"
    hma_summary_flayer = nifc_gis.content.get(hma_summary_item_id)
    hma_summary_layer = hma_summary_flayer.layers[0]
    local_fields = [f.name for f in arcpy.ListFields(hma_join_outds)]
    agol_fields = [f['name'] for f in hma_summary_layer.properties.fields]
    write_to_AGOL(hma_summary_layer, hma_join_outds)

    # Create a new log record with this time/date on AGOL to show in Dashboard
    #Clear the existing NIFC AGOL item of data
    edit_htab_log.manager.truncate()
    #Write date
    print("...Add a new record to the log file with this time/date: "+datetmstr)
    add_feature = {"attributes" : {"LastUpdated":dt}}
    edit_htab_log.edit_features(adds=[add_feature])

    # Add a line of text to the log text file in the EGIS directory
    #Update log file
    lstmsg = [sma_st_msg,blm_irwin_msg,grsg_msg,gusg_msg]
    success = "SUCCESS"
    error_str = ""
    for msg in lstmsg:
        if msg[0:7] == "Problem":
            success = "PARTIAL FAIL"
        elif msg[0:10] == "No records":
            success = "POSSIBLE FAIL"
    print("UPDATING EGIS LOG FILE WITH A "+success)
    log_update_list = [datetmstr,success,sma_st_msg,blm_irwin_msg,grsg_msg,gusg_msg,log_tbs_msg,error_str]
    log_df.loc[len(log_df)] = log_update_list
    log_df.to_csv(logfile, index=False)
   
except Exception as e:    
    error_str = str(e)
    print("ERROR:", error_str)

    print("UPDATING EGIS LOG FILE WITH A FAIL")
    log_update_list = [datetmstr,"FAIL",sma_st_msg,blm_irwin_msg,grsg_msg,gusg_msg,log_tbs_msg,error_str]
    log_df.loc[len(log_df)] = log_update_list
    log_df.to_csv(logfile, index=False)
if arcpy.Exists("in_memory"):
    arcpy.Delete_management("in_memory")
#report end time
dte = datetime.now()
datetmend = dte.strftime("%Y%m%d_%H%M")
print("END TIME: "+datetmend)
