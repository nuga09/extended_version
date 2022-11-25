# -*- coding: utf-8 -*-
"""
Created on Tue May 31 12:34:48 2022

@author: d.nuga
"""
import os
from zipfile import ZipFile
import shutil
import glob 
import pandas as pd 
import sqlite3
import requests
from tqdm import tqdm
import chardet
from sqlalchemy import create_engine
from bs4 import BeautifulSoup


html = requests.get("https://www.marktstammdatenregister.de/MaStR/Datendownload")
soup = BeautifulSoup(html.text, "lxml")
# find the download button element on the website
element = soup.find_all("a", "btn btn-primary text-right")[0]
# extract the url from the html element
link = str(element).split('href="')[1].split('" title')[0]
print(link)

def mstr_extraction(url=link):
    #setting file name for zip file 
    filename = url.split('/')[-1]
    db_file = filename.split('.')[0]
    date = db_file.split('_')[1]
    print(db_file, date)
    
    if not os.path.isdir('Mstr'):
        #creating directory for file download
        directory = os.mkdir('Mstr')        
    else:
        pass 
     if not os.path.isdir('db_Storage'):
        #creating directory for saving files
        directory = os.mkdir('db_Storage')        
    else:
        pass 
    if len(os.listdir('Mstr'))== 0:            
        print("Directory is empty")           
        
        #Streaming, to so we can iterate over the response.                
        response = requests.get(url, stream=True)
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        
        with open('Mstr/Gesamtdatenexport.zip', 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        print('\nDownloading Completed')
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")
       
    elif len(os.listdir('Mstr'))== 1 :
        #Extract all the contents of zip file in current directory
        folder = 'Mstr/'
       
        
        for item in os.listdir(folder):
            print(item)
            with ZipFile(os.path.join(folder,item), 'r') as zipObj:
               zipObj.extractall(folder) 
               
    else:
        #load_df = pd.read_csv('Mstr/Total_Gesamtdatenexport_20220609_V1.csv')  
        
        data_from_trep_db = []        
        column_names_from_trep_db = ['index', 'ENH_MastrID', 'ENH_MastrNummer', 'ENH_Systemstatus', 'ENH_Betriebsstatus', 'ENH_EinheitenTyp', 'ENH_NameEinheit', 'ENH_NameWindpark', 'ENH_NameKraftwerk', 'ENH_NameKraftwerksblock', 'ENH_VoruebergehendeStilllegungBeginn', 'ENH_WiederInbetriebnahmeDatum', 'ENH_GeplantesInbetriebnahmeDatum', 'ENH_InbetriebnahmeDatum', 'ENH_EndgueltigeStilllegungDatum', 'ENH_Meldedatum', 'ENH_LetzteAktualisierung', 'ENH_Land', 'ENH_Lage', 'ENH_Plz', 'ENH_Ort', 'ENH_Standortangabe', 'ENH_Strasse', 'ENH_Hausnummer', 'ENH_Adresszusatz', 'ENH_Gemarkung', 'ENH_Flurstueck', 'ENH_Bundesland', 'ENH_Landkreis', 'ENH_Gemeinde', 'ENH_Gemeindeschluessel', 'ENH_Seelage', 'ENH_ClusterOstsee', 'ENH_ClusterNordsee', 'ENH_Kuestenentfernung', 'ENH_Wassertiefe', 'ENH_Breitengrad', 'ENH_Laengengrad', 'ENH_AnzahlModule', 'ENH_Bruttoleistung', 'ENH_Nettonennleistung', 'ENH_Wechselrichterleistung', 'ENH_Wechselrichter', 'ENH_Leistungsbegrenzung', 'ENH_IsGrenzkraftwerk', 'ENH_NettonennleistungDeutschland', 'ENH_IsKombibetrieb', 'ENH_NettoleistungSteigerungDurchKombibetrieb', 'ENH_KombibetriebMastrNummer', 'ENH_SolarLage', 'ENH_Nutzungsbereich', 'ENH_EinheitlicheAusrichtungUndNeigungswinkel', 'ENH_HauptAusrichtung', 'ENH_HauptNeigungswinkel', 'ENH_NebenAusrichtung', 'ENH_NebenNeigungswinkel', 'ENH_InAnspruchgenommeneFlaeche', 'ENH_ArtDerFlaeche', 'ENH_InAnspruchgenommeneAckerflaeche', 'ENH_Technologie', 'ENH_Nabenhoehe', 'ENH_Rotordurchmesser', 'ENH_AuflagenZuAbschaltungBzwLeistungsgrenzen', 'ENH_Hersteller', 'ENH_Typenbezeichner', 'ENH_Energietraeger', 'ENH_Hauptbrennstoff', 'ENH_HauptbrennstoffWeiterer', 'ENH_NetzreserveDatum', 'ENH_SicherheitsbereitschaftDatum', 'ENH_Baubeginn', 'ENH_Biomasseart', 'ENH_ArtDerWasserkraftanlage', 'ENH_ZuflussArt', 'ENH_CanStromerzeugungMindern', 'ENH_MarktpartnerNummer', 'ENH_IsFernsteuerbarNetzbetreiber', 'ENH_IsFernsteuerbarDirektvermarkter', 'ENH_IsFernsteuerbarDritte', 'ENH_IsAngeschlossenAnHoechstOderHochspannungsnetz', 'ENH_EinspeisungsArt', 'ENH_AnteiligNutzungsberechtigte', 'ENH_Einsatzort', 'ENH_MaStRNummerNetzbetreiber', 'ENH_IsStilllegungNachParagraph13b', 'ENH_ArtDerStilllegungNachParagraph13b', 'ENH_BeginnDerStilllegungNachParagraph13b', 'ENH_EndeDerStilllegungNachParagraph13b', 'ENH_WCode', 'ENH_WCodeDisplayname', 'ENH_Kraftwerksnummer', 'ENH_ArtAbschaltbareLast', 'ENH_AnteilBeeinflussbarerLast', 'ENH_AnzahlAngeschlossenerStromverbrauchseinheitenUeber50MegaWatt', 'ENH_IsPraequalifiziertAbschaltbareLast', 'ENH_Erzeugungsleistung', 'ENH_IsStromerzeugung', 'ENH_MastrNummernDerZugehoerigenStromerzeugungseinheiten', 'ENH_MaximaleGasbezugsleistung', 'ENH_Kopplung', 'ENH_Batterietechnologie', 'ENH_LeistungsaufnahmePumpbetrieb', 'ENH_IsRegelbetriebPumpbetrieb', 'ENH_IsNotstromaggregat', 'ENH_NBP_Pruefungsdatum', 'ENH_NBP_Netzbetreiberpruefungsstatus']
        # `mastr_names_2_trep_names={
        # "InstallierteLeistung":"ENH_Nettonennleistung",
        # "Plz":"ENH_Plz",
        # "Betriebsstatus":"ENH_Betriebsstatus",
        # "InbetriebnahmeDatum":"ENH_InbetriebnahmeDatum",
        # "Rotordurchmesser":"ENH_Rotordurchmesser",
        # "Nabenhoehe":"ENH_Nabenhoehe",
        # "Breitengrad":"ENH_Breitengrad",
        # "Laengengrad":"ENH_Laengengrad",
        # "Seelage":"ENH_Seelage",
        # "EinheitenTyp":"ENH_EinheitenTyp",
        # "":"ENH_Betriebsstatus",
        # "MastrID":"ENH_MastrID",
        # "Lage":"ENH_Lage",
        # "Bundesland":"ENH_Bundesland"
        # }
        # new_mastr=pd.DataFrame()
        # new_mastr=new_mastr.rename(columns=mastr_names_2_trep_names)`
        for x in column_names_from_trep_db:
            s =(x.split('_')[-1])
            data_from_trep_db.append(s)
        print(sorted(data_from_trep_db))
        
        all_xml_data = []
        xml_reader = glob.glob('Mstr/*.xml')
        
        #obtaining the file encoding to read XML and concatenate with pandas
        
        for x in xml_reader:
            with open(x, 'rb') as file:
                print(chardet.detect(file.read()))
            df = pd.read_xml(x, encoding='utf-16')
            #print(sorted(df.columns))
            all_xml_data.append(df)
        new_df = pd.concat(all_xml_data)
        print('all data concatenated', new_df)
        new_df.columns = [i.replace('Eeg','') if i.startswith('Eeg') else i.replace('Netz','') if i.startswith('Netz') else i for i in new_df.columns]        
        #new_df.columns = [f'{col}' for col in new_df.columns]
        s = list(new_df.columns)
        print(sorted(s))
        match = [i for i in data_from_trep_db if i in new_df.columns]
        print(match)        
        
        mini_df = new_df[[i for i in data_from_trep_db if i in new_df.columns]]
        mini_df.columns = [f'ENH_{col}' for col in mini_df.columns]
        print('extracted columns',mini_df.columns)
        mini_df.to_csv(f'db_Storage/{db_file}.csv', sep='\t')
        
        conn = None
        try:
            conn = sqlite3.connect(f'db_Storage/{db_file}')
            print(sqlite3.version)
            engine = create_engine(f'db_Storage/sqlite:///Mstr_{db_file}.db') 
            #c = con.cursor()
            #c.execute(f'CREATE TABLE IF NOT EXISTS Process ({new_df.columns})')
            #con.commit()
            new_df.to_sql(f'db_Storage/processed_mastr_{date}', con = engine, if_exists='append', index=False)
            
            
            mini_df.to_sql(f'db_Storage/part_{date}', con=engine, if_exists = 'append',index=False) 
        except Error as e:
            print(e)
        finally:
            if conn:
                conn.close()
        
mstr_extraction() 
        

    



    
    