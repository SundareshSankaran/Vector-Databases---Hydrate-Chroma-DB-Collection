/* templated code goes here*/;

/* -----------------------------------------------------------------------------------------* 
   Vector Databases - Hydrate Chroma DB Collection

   Version: 1.0 (24JAN2024)
   Created: Sundaresh Sankaran(sundaresh.sankaran@sas.com)
    
   Available at: 
   https://github.com/SundareshSankaran/Vector-Databases---Hydrate-Chroma-DB-Collection
*------------------------------------------------------------------------------------------ */

/*-----------------------------------------------------------------------------------------*
   START MACRO DEFINITIONS.
*------------------------------------------------------------------------------------------*/

/* -----------------------------------------------------------------------------------------* 
   Macro to create an error flag for capture during code execution.

   Input:
      1. errorFlagName: The name of the error flag you wish to create. Ensure you provide a 
         unique value to this parameter since it will be declared as a global variable.

    Output:
      2. &errorFlagName : A global variable which takes the name provided to errorFlagName.

   Also available at: 
   https://github.com/SundareshSankaran/sas_utility_programs/blob/main/code/Error%20Flag%20Creation/macro_create_error_flag.sas
*------------------------------------------------------------------------------------------ */


%macro _create_error_flag(errorFlagName);

   %global &errorFlagName.;
   %let  &errorFlagName.=0;

%mend _create_error_flag;



/* -------------------------------------------------------------------------------------------* 
   Macro to initialize a run-time trigger global macro variable to run SAS Studio Custom Steps. 
   A value of 1 (the default) enables this custom step to run.  A value of 0 (provided by 
   upstream code) sets this to disabled.

   Input:
   1. triggerName: The name of the runtime trigger you wish to create. Ensure you provide a 
      unique value to this parameter since it will be declared as a global variable.

   Output:
   2. &triggerName : A global variable which takes the name provided to triggerName.
   
   Also available at:
   https://github.com/SundareshSankaran/sas_utility_programs/blob/main/code/Create_Run_Time_Trigger/macro_create_runtime_trigger.sas
*-------------------------------------------------------------------------------------------- */

%macro _create_runtime_trigger(triggerName);

   %global &triggerName.;

   %if %sysevalf(%superq(&triggerName.)=, boolean)  %then %do;
  
      %put NOTE: Trigger macro variable &triggerName. does not exist. Creating it now.;
      %let &triggerName.=1;

   %end;

%mend _create_runtime_trigger;


/*-----------------------------------------------------------------------------------------*
   Macro variable to capture indicator of a currently active CAS session
*------------------------------------------------------------------------------------------*/

%global casSessionExists;
%global _current_uuid_;

/*-----------------------------------------------------------------------------------------*
   Macro to capture indicator and UUIDof any currently active CAS session.
   UUID is not expensive and can be used in future to consider graceful reconnect.
*------------------------------------------------------------------------------------------*/

%macro _cas_checkSession;
   %if %sysfunc(symexist(_SESSREF_)) %then %do;
      %let casSessionExists= %sysfunc(sessfound(&_SESSREF_.));
      %if &casSessionExists.=1 %then %do;
         proc cas;
            session.sessionId result = sessresults;
            call symputx("_current_uuid_", sessresults[1]);
            %put NOTE: A CAS session &_SESSREF_. is currently active with UUID &_current_uuid_. ;
         quit;
      %end;
   %end;
%mend _cas_checkSession;


/*-----------------------------------------------------------------------------------------*
   This macro creates a global macro variable called _usr_nameCaslib
   that contains the caslib name (aka. caslib-reference-name) associated with the libname 
   and assumes that the libname is using the CAS engine.

   As sysvalue has a length of 1024 chars, we use the trimmed option in proc sql
   to remove leading and trailing blanks in the caslib name.
   From macro provided by Wilbram Hazejager
*------------------------------------------------------------------------------------------*/

%macro _usr_getNameCaslib(_usr_LibrefUsingCasEngine); 

   %global _usr_nameCaslib;
   %let _usr_nameCaslib=;

   proc sql noprint;
      select sysvalue into :_usr_nameCaslib trimmed from dictionary.libnames
      where libname = upcase("&_usr_LibrefUsingCasEngine.") and upcase(sysname)="CASLIB";
   quit;

%mend _usr_getNameCaslib;

/*-----------------------------------------------------------------------------------------*
   EXECUTION CODE MACRO 
*------------------------------------------------------------------------------------------*/

%macro _nctf_main_execution_code;

/*-----------------------------------------------------------------------------------------*
   Create an error flag. 
*------------------------------------------------------------------------------------------*/

   %_create_error_flag(_nctf_error_flag);

/*-----------------------------------------------------------------------------------------*
   Check if an active CAS session exists. 
*------------------------------------------------------------------------------------------*/

   %_nctf_checkSession;

   %if &casSessionExists. = 0 %then %do;
      %put ERROR: A CAS session does not exist. Connect to a CAS session upstream. ;
      %let _nctf_error_flag = 1;
   %end;
   %else %do;

/*-----------------------------------------------------------------------------------------*
   Check Input table libref to ensure it points to a valid caslib.
*------------------------------------------------------------------------------------------*/

      %if &_nctf_error_flag. = 0 %then %do;

         %global inputCaslib;
   
         %_usr_getNameCaslib(&inputTable_lib.);
         %let inputCaslib=&_usr_nameCaslib.;
         %put NOTE: &inputCaslib. is the caslib for the input table.;
         %let _usr_nameCaslib=;

         %if "&inputCaslib." = "" %then %do;
            %put ERROR: Input table caslib is blank. Check if Base table is a valid CAS table. ;
            %let _nctf_error_flag=1;
         %end;

      %end;

%mend;

cas ss;
caslib _all_ assign;

proc python;

submit;

__import__('pysqlite3')
import sys
sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')

import swat, os
os.environ['CAS_CLIENT_SSL_CA_LIST']=os.environ['SSLCALISTLOC']
conn = swat.CAS(hostname="sas-cas-server-default-client",port=5570, password=os.environ['SAS_SERVICES_TOKEN'])


import chromadb

chroma_client = chromadb.Client()

print(chroma_client.heartbeat())

collection = chroma_client.get_or_create_collection(name="newcollection")

print(collection.count())

import re
scoredTable = conn.CASTable(name="Topics_20240123", caslib="PUBLIC")
# columnlist = list(filter(lambda x: re.search(r'_Col', x), scoredTable.columns.to_list()))

import pandas
df = pandas.DataFrame()
df['Embeddings'] = (
    scoredTable.to_frame().filter(like='_Col')
      .apply(lambda row: row.dropna().tolist(), axis=1)
)

# Embed and store the reviews
insertion = collection.add(
    ids=[str(i) for i in scoredTable['Id_review']],  
    documents=[doc for doc in scoredTable["Text_Review"]],
    embeddings=[embedding for embedding in df["Embeddings"]],
    metadatas=[{"rating": target} for target in scoredTable["Target_Rating"]],
)

SAS.logMessage("The collection contains {} documents.".format(collection.count()))

endsubmit;


quit;


cas ss terminate;