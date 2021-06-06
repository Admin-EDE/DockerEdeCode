# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger('root');

from validate_email import validate_email
import re
from itertools import cycle
from datetime import datetime
from pytz import timezone 
import os
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine

#----------------------------------------------------------------------------
#PASO N° 20 - Transformar el archivo JSON en archivos CSV's. Uno por tabla.
#----------------------------------------------------------------------------

class check:
  def __init__(self, args):
    self.args = args;
    logger.info(f"typo de argumento: {type(self.args)}, valores: {self.args}");
    self.functions = {
      "fn0FA": "No/Verificado",
      "fn0FB": "No/Verificado",
      "fn1FA": "No/Verificado",
      "fn1FB": "No/Verificado",
      "fn1FC": "No/Verificado",
      "fn2FA": "No/Verificado",
      "fn2EA": "No/Verificado",
      "fn2DA": "No/Verificado",
      "fn2DB": "No/Verificado",
      "fn2CA": "No/Verificado",
      "fn2CB": "No/Verificado",
      "fn2BA": "No/Verificado",
      "fn2AA": "No/Verificado",
      "fn29A": "No/Verificado",
      "fn29B": "No/Verificado",
      "fn29C": "No/Verificado",
      "fn28A": "No/Verificado",
      "fn28B": "No/Verificado",
      "fn3F0": "self.fn3F0(conn)",
      "fn3F1": "self.fn3F1(conn)",
      "fn3F2": "self.fn3F2(conn)",
      "fn3F3": "self.fn3F3()",
      "fn3F4": "self.fn3F4()",
      "fn3F5": "self.fn3F5()",
      "fn3F6": "self.fn3F6()",
      "fn3F7": "self.fn3F7()",
      "fn3F8": "self.fn3F8()",
      "fn3F9": "self.fn3F9()",
      "fn3FA": "self.fn3FA()",
      "fn3FB": "self.fn3FB()",
      "fn3FC": "self.fn3FC()",
      "fn3FD": "self.fn3FD()",
      "fn3FE": "self.fn3FE(conn)",
      "fn3FF": "self.fn3FF()",
      "fn3E0": "self.fn3E0(conn)",
      "fn3E1": "self.fn3E1()",
      "fn3E2": "self.fn3E2(conn)",
      "fn3E3": "self.fn3E3()",
      "fn3E4": "self.fn3E4(conn)",
      "fn3E5": "self.fn3E5()",
      "fn3E6": "self.fn3E6()",
      "fn3E7": "self.fn3E7()",
      "fn3E8": "self.fn3E8()",
      "fn3E9": "self.fn3E9()",
      "fn3EA": "self.fn3EA()",
      "fn3EB": "self.fn3EB()",
      "fn3EC": "self.fn3EC()",
      "fn3ED": "self.fn3ED()",
      "fn3EE": "self.fn3EE()",
      "fn3EF": "self.fn3EF(conn)",
      "fn3D0": "No/Verificado",
      "fn3D1": "No/Verificado",
      "fn3D2": "No/Verificado",
      "fn3D3": "No/Verificado",
      "fn3D4": "No/Verificado",
      "fn3D5": "No/Verificado",
      "fn3D6": "No/Verificado",
      "fn3D7": "No/Verificado",
      "fn3D8": "No/Verificado",
      "fn4FA": "self.fn4fa(conn,idorga)",
      "fn5E0": "self.fn5f0(conn,fecha,organizacion)",
      "fn5E0": "No/Verificado",
      "fn5E1": "No/Verificado",
      "fn5E2": "No/Verificado",
      "fn5E3": "No/Verificado",
      "fn5E4": "self.fn5E4(conn)",
      "fn5E5": "self.fn5E5(conn)",
      "fn5E6": "No/Verificado",
      "fn5D0": "self.fn5D0(conn date)",
      "fn6F0": "No/Verificado",
      "fn6F1": "No/Verificado",
      "fn6E0": "No/Verificado",
      "fn6E1": "No/Verificado",
      "fn6E2": "No/Verificado",
      "fn6E3": "No/Verificado",
      "fn6E4": "No/Verificado",
      "fn6E5": "No/Verificado",
      "fn6D0": "No/Verificado",
      "fn6D1": "No/Verificado",
      "fn6C0": "No/Verificado",
      "fn6C1": "No/Verificado",
      "fn6C2": "No/Verificado",
      "fn6B0": "No/Verificado",
      "fn6B1": "No/Verificado",
      "fn6A0": "No/Verificado",
      "fn6A1": "No/Verificado",
      "fn6A2": "No/Verificado",
      "fn6A3": "No/Verificado",
      "fn690": "No/Verificado",
      "fn680": "No/Verificado",
      "fn681": "No/Verificado",
      "fn682": "No/Verificado",
      "fn7F0": "No/Verificado",
      "fn7F1": "No/Verificado",
      "fn7F2": "No/Verificado",
      "fn7F3": "No/Verificado",
      "fn7F4": "No/Verificado",
      "fn7F5": "No/Verificado",
      "fn7F6": "No/Verificado",
      "fn8F0": "No/Verificado",
      "fn8F1": "self.fn8F1(conn, idIncident)",
      "fn8F2": "self.fn8F2(conn)",
      "fn8F3": "self.fn8F3(conn)",
      "fn9F0": "No/Verificado",
      "fn9F1": "No/Verificado",
      "fn9F2": "No/Verificado",
      "fn9F3": "No/Verificado"	
    }
    # self.dfLog = dfLog
    # self._encode = _encode
    # self._sep = _sep
    # self.secPhase = secPhase
    # self.path_to_DB_file = path_to_DB_file
    #t_stamp = datetime.timestamp(datetime.now(timezone('Chile/Continental')))        
    self.args._FKErrorsFile = f'./{self.args.t_stamp}_ForenKeyErrors.csv';
    self.listValidations = self.cargarPlanillaConListasParaValidar();

  #----------------------------------------------------------------------------
  # Transforma archivo JSON en un DataFrame de pandas con todas sus columnas.
  # Agrega las columnas que faltan.
  #----------------------------------------------------------------------------
  def execute(self):
    _result = True
    engine = create_engine(f"sqlite+pysqlcipher://:{self.args.secPhase}@/{self.args.path_to_DB_file}?cipher=aes-256-cfb&kdf_iter=64000")
    try:
      conn = engine.connect()

      for key,value in self.functions.items():
        if(value != "No/Verificado"):
          logger.info(f"Ejecutando función {key} con los parámetros {value}")
          eval_ = eval(value)
          logger.info(f"Resultado de la evaluación de la función {key}: {eval_}")
          _result = eval_ and _result

      #_r[0] = self.verificaIntegridadReferencial(conn,self.args._FKErrorsFile)
      #self.verificaDatosDeLasPersonas(conn)
      #self.verificaDatosEstudiantes(conn)
      #self.verificaDatosDocentes(conn)    
      #self.verificaDatosEstablecimiento(conn)
      #self.verificaJerarquiasOrganizacional(conn)
      #self.verificaClaveAleatoriaDocentes(conn)

      if(not _result):
        raise Exception("El archivo no cumple con el Estándar de Datos para la Educación. Hay errores en la revisión. Revise el LOG para más detalles")

    except Exception as e:
      _t = "ERROR en la validación: "+str(e)
      logger.info(_t);
      _result = False
    finally:
      #closind database connection
      conn.close()
    return True#_result
    
  #Carga planilla con todas las listas de validación desde Google Drive
  #https://drive.google.com/open?id=1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s
  def cargarPlanillaConListasParaValidar(self):
    #idFile = '1vZD8ufVm3Z71V9TveQcLI0A02wrmwsz43z3TyWl9C-s'
    #url = f'https://docs.google.com/spreadsheets/d/{idFile}/export?format=xlsx&id={idFile}'
    url = './ede/ede/listValidationData.xlsx';
    xd = pd.read_excel(url,'ListValidations')
    _t=f'Planilla {url} cargada satisfactoriamente'; logger.info(_t)
    return xd;

  #VERIFICA LA CONEXION A LA BASE DE DATOS
  def fn3F0(self,conn):
    _r = False;
    rows = conn.execute("SELECT * FROM PersonList;").fetchall()
    if(len(rows)>0): _r = True
    logger.info("Aprobado") if _r else logger.error("Rechazado");
    return _r

  #VERIFICA LA INTEGRIDAD REFERENCIAL DE LOS DATOS
  def fn3F1(self,conn):
    _r = True; _e = ''
    rows = conn.execute("PRAGMA foreign_key_check;")

    if(rows.returns_rows):
      pd.DataFrame(rows,columns=['Table', 'rowId', 'Parent', 'FkId']).to_csv(
          self.args._FKErrorsFile,sep=self.args._sep,encoding=self.args._encode,index=False)
      _e = f"BD con errores, más detallen en {self.args._FKErrorsFile}"
      _r = False
    
    logger.info(f"RESULTADO DE LA VERIFICACIÓN DE LA INTEGRIDAD REFERENCIAL: {_r}. {_e}");
    logger.error("Rechazado") if (_e != '') else logger.info("Aprobado")
    return _r

  #VERIFICA SI LA VISTA PersonList contiene información
  def fn3F2(self, conn):
    try:
      rows = conn.execute("""
        SELECT 
          RUN
          ,IPE
          ,emailAddress
          ,emailAddressType
          ,TelephoneNumber 
          ,telephoneNumberType
          ,numeroListaCurso    
          ,numeroMatricula
          ,fechaIncorporacionEstudiante
          ,fechaRetiroEstudiante
          ,fechaCumpleanos
          ,TribalAffiliationDescription
        FROM PersonList;
      """).fetchall()

      logger.info(f"len(personList): {len(rows)}")

      if(len(rows)>0):
        self.rutList = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None])) # Valida lista de rut ingresados a la BD  
        self.ipeList = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None])) # Valida lista de IPE ingresados a la BD  
        self.emailList = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None])) #Valida que los email tengan el formato correcto
        self.emailTypeList = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None])) #Valida que los emailType tengan el formato correcto
        self.phoneList = self.convertirArray2DToList(list([m[4] for m in rows if m[4] is not None])) # Valida que los teléfonos cumplan con el formato E164
        self.phoneTypeList = self.convertirArray2DToList(list([m[5] for m in rows if m[5] is not None])) # Valida que los tipos de teléfonos cumplan con el formato E164
        self.numListaList = self.convertirArray2DToList(list([m[6] for m in rows if m[6] is not None])) # Valida los número de lista de los cursos sean número
        self.numMatriculaList = self.convertirArray2DToList(list([m[7] for m in rows if m[7] is not None])) # Valida los números de matrícula del establecimiento sean número
        self.fechaIncorporacionEstudianteList = self.convertirArray2DToList(list([m[8] for m in rows if m[8] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.fechaRetiroEstudianteList = self.convertirArray2DToList(list([m[9] for m in rows if m[9] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.fechaCumpleanosList = self.convertirArray2DToList(list([m[10] for m in rows if m[10] is not None])) # Valida que las fechas cumplan con el formato de fecha
        self.AllDatesList = self.fechaIncorporacionEstudianteList+self.fechaRetiroEstudianteList+self.fechaCumpleanosList # Valida que las fechas cumplan con el formato de fecha
        self.TribalList = self.convertirArray2DToList(list(set([m[11] for m in rows if m[11] is not None]))) # Valida que las afiliaciones tribales sean las permitidas en Chile
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE RUN INGRESADAS EN EL SISTEMA ES VALIDA
  def fn3F3(self):
    try:
      _l = self.rutList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validarRUN(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL RUN DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE IPE INGRESADOS EN EL SISTEMA ES VALIDA
  def fn3F4(self):
    try:
      _l = self.ipeList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validarIpe(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL IPE DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE e-mails INGRESADOS EN EL SISTEMA CUMPLE CON EL FORMATO
  def fn3F5(self):
    try:
      _l = self.emailList
      if(len(_l)>0):
        _err = set([e for e in _l if not validate_email(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DE FORMATO DE LOS E-MAILS DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE teléfonos INGRESADOS cumple con el formato E164
  def fn3F6(self):
    try:
      _l = self.phoneList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoE164Telefono(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DE LOS TELEFONOS DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI EL NUMERO DE LISTA cumple con el formato
  def fn3F7(self):
    try:
      _l = self.numListaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoNumero(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DEL NUMERO DE LISTA DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI EL NUMERO DE MATRICULA cumple con el formato
  def fn3F8(self):
    try:
      _l = self.numMatriculaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoNumero(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL NUMERO DE MATRICULA DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE FECHAS INGRESADAS cumple con el formato
  def fn3F9(self):
    try:
      _l = self.AllDatesList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoFecha(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DE LAS DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA LISTA DE afiliaciones tribales se encuentra dentro de la lista permitida
  def fn3FA(self):
    try:
      _l = self.TribalList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaTribalAffiliation(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DE LA LISTA DE AFILIACIONES TRIBALES DE LAS PERSONAS: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #Valida que la cantidad de #matricula == #Lista == #fechasIncorporaciónes
  def fn3FB(self):
    try:
      _l1 = self.numListaList;_l2 = self.numMatriculaList;_l3 = self.fechaIncorporacionEstudianteList;
      if(len(_l1)>0 and len(_l2)>0 and len(_l3)>0):
        _r   = len(_l1) == len(_l2) == len(_l3)
        _t = f"Valida que la cantidad de #matricula == #Lista == #fechasIncorporaciónes: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA que la cantidad de emails corresponda con los tipos de emails ingresados
  def fn3FC(self):
    try:
      _l1 = self.emailList;_l2 = self.emailTypeList
      if(len(_l1)>0 and len(_l2)>0):
        _r   = len(_l1) == len(_l2)
        _t = f"VERIFICA que la cantidad de e-mails corresponda con los tipos de e-mails ingresados en las personas: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA que la cantidad de teléfonos corresponda con los tipos de teléfonos ingresados
  def fn3FD(self):
    try:
      _l1 = self.phoneList;_l2 = self.phoneTypeList
      if(len(_l1)>0 and len(_l2)>0):
        _r   = len(_l1) == len(_l2)
        _t = f"VERIFICA que la cantidad de teléfonos corresponda con los tipos de teléfonos ingresados en las personas: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA PersonList filtrada por estudiantes contiene información
  def fn3FE(self, conn):
    try:
      rows = conn.execute("""
      SELECT 
        personId
        ,ciudadNacimiento
        ,regionNacimiento
        ,paisNacimiento
      FROM PersonList
      WHERE Role like '%Estudiante%';
    """).fetchall()

      logger.info(f"len(estudiantes): {len(rows)}")

      if(len(rows)>0):
        personId  = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None]))
        cuidadNac = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None]))
        regionNac = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None]))
        paisNac   = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None]))
        self.comparaEstudiantes = [len(personId) == len(cuidadNac) == len(regionNac) == len(paisNac)]
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por estudiantes: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE TODOS LOS ESTUDIANTES TENGAN Pais, Región y cuidad de nacimiento
  def fn3FF(self):
    try:
      _l = self.comparaEstudiantes
      if(len(_l)>0):
        _r   = _l
        _t = f"VERIFICA QUE TODOS LOS ESTUDIANTES TENGAN Pais, Región y cuidad de nacimiento: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA PersonList filtrada por docentes contiene información
  def fn3E0(self, conn):
    try:
      rows = conn.execute("""
      SELECT 
        personId
        ,DegreeOrCertificateTitleOrSubject
        ,DegreeOrCertificateTypeDescription
        ,AwardDate
        ,NameOfInstitution
        ,higherEducationInstitutionAccreditationStatusDescription
        ,educationVerificationMethodDescription
      FROM PersonList
      WHERE Role like '%Docente%';
    """).fetchall()

      logger.info(f"len(docentes): {len(rows)}")

      if(len(rows)>0):
        personId            = self.convertirArray2DToList(list([m[0] for m in rows if m[0] is not None]))
        title               = self.convertirArray2DToList(list([m[1] for m in rows if m[1] is not None]))
        Type                = self.convertirArray2DToList(list([m[2] for m in rows if m[2] is not None]))
        AwardDate           = self.convertirArray2DToList(list([m[3] for m in rows if m[3] is not None]))
        Institution         = self.convertirArray2DToList(list([m[4] for m in rows if m[4] is not None]))
        AccreditationStatus = self.convertirArray2DToList(list([m[5] for m in rows if m[5] is not None]))
        VerificationMethod  = self.convertirArray2DToList(list([m[6] for m in rows if m[6] is not None]))
        self.comparaDocentes = [len(personId) == len(title) == len(Type) == len(AwardDate) == len(Institution) == len(AccreditationStatus) == len(VerificationMethod)]
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por docentes: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema
  def fn3E1(self):
    try:
      _l = self.comparaDocentes
      if(len(_l)>0):
        _r   = _l
        _t = f"VERIFICA QUE TODOS LOS DOCENTES TENGAN su título y la institución de educación ingresados en el sistema: {_r}.";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA TABLA k12schoolList unida a organizationList contiene información
  def fn3E2(self, conn):
    try:
      rows = conn.execute("SELECT Identifier FROM k12schoolList INNER JOIN organizationList USING(OrganizationId);").fetchall()
      logger.info(f"len(establecimientos): {len(rows)}")
      if(len(rows)>0):
        self.formatoRBD = self.convertirArray2DToList(list(set([m[0] for m in rows if m[0] is not None])))
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la tabla k12schoolList para identificar el RBD del establecimiento: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL FORMATO DEL RBD CORRESPONDA
  def fn3E3(self):
    try:
      _l = self.formatoRBD
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaFormatoRBD(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICACION DEL FORMATO DEL RBD DEL ESTABLECIMIENTO: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA SI LA VISTA jerarquiasList contiene información
  def fn3E4(self, conn):
    try:
      rows = conn.execute("SELECT * FROM jerarquiasList;").fetchall()
      logger.info(f"len(organizaciones): {len(rows)}")
      if(len(rows)>0):
        self.modalidadList = self.convertirArray2DToList(list(set([m[2] for m in rows if m[2] is not None])))
        self.jornadaList = self.convertirArray2DToList(list(set([m[3] for m in rows if m[3] is not None])))
        self.nivelList = self.convertirArray2DToList(list(set([m[4] for m in rows if m[4] is not None])))
        self.ramaList = self.convertirArray2DToList(list(set([m[5] for m in rows if m[5] is not None])))
        self.sectorList = self.convertirArray2DToList(list(set([m[6] for m in rows if m[6] is not None])))
        self.especialidadList = self.convertirArray2DToList(list(set([m[7] for m in rows if m[7] is not None])))
        self.tipoCursoList = self.convertirArray2DToList(list(set([m[8] for m in rows if m[8] is not None])))
        self.codigoEnseList = self.convertirArray2DToList(list(set([m[9] for m in rows if m[9] is not None])))
        self.gradoList = self.convertirArray2DToList(list(set([m[10] for m in rows if m[10] is not None])))
        self.letraCursoList = self.convertirArray2DToList(list(set([m[11] for m in rows if m[11] is not None])))
        logger.info(f"Aprobado")
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA MODALIDAD ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3E5(self):
    try:
      _l = self.modalidadList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaModalidad(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA MODALIDAD ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA JORNADA ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3E6(self):
    try:
      _l = self.jornadaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaJornada(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA JORNADA ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL NIVEL ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E7(self):
    try:
      _l = self.nivelList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaNivel(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL NIVEL ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA RAMA ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E8(self):
    try:
      _l = self.ramaList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaRama(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA RAMA ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL SECTOR ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3E9(self):
    try:
      _l = self.sectorList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaSector(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL SECTOR ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA ESPECIALIDAD ESTA DENTRO DE LA LISTA PERMITIDA
  def fn3EA(self):
    try:
      _l = self.especialidadList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaEspecialidad(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA ESPECIALIDAD ESTA DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL TIPO DE CURSO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EB(self):
    try:
      _l = self.tipoCursoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaTipoCurso(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL TIPO DE CURSO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL CODIGO DE ENSEÑANZA ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EC(self):
    try:
      _l = self.codigoEnseList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaCodigoEnse(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL CODIGO DE ENSEÑANZA ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3ED(self):
    try:
      _l = self.gradoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaGrado(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE EL GRADO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE LA LETRA DEL CURSO ESTE DENTRO DE LA LISTA PERMITIDA
  def fn3EE(self):
    try:
      _l = self.letraCursoList
      if(len(_l)>0):
        _err = set([e for e in _l if not self.validaLetraCurso(e)])
        _r   = False if len(_err)>0 else True
        _t = f"VERIFICA QUE LA LETRA DEL CURSO ESTE DENTRO DE LA LISTA PERMITIDA: {_r}. {_err}";
        logger.info(_t) if _r else logger.error(_t)
        logger.info(f"Aprobado") if _r else logger.error(f"Rechazado")
      else:
        logger.info("S/Datos")
      return True
    except Exception as e:
      logger.error(f"No se pudo ejecutar la verificación: {str(e)}")
      logger.error(f"Rechazado")
      return False

  #VERIFICA QUE COINCIDAN LOS ID'S DE LOS CURSOS EN LAS DIFERENTES TABLAS
  def fn3EF(self, conn):
    try:
      courses1 = conn.execute("SELECT OrganizationIdDelCurso FROM jerarquiasList;").fetchall()
      courses2 = conn.execute("SELECT OrganizationIdCurso FROM cursoList;").fetchall()
      logger.info(f"Vista jerarquiasList: {len(courses1)}, Vista cursoList: {len(courses2)}")
      if(len(courses1)>0 and len(courses2)>0):
        # Valida que lista de cursos coincidan
        curso1 = list(set([m[0] for m in courses1 if m[0] is not None]))
        curso2 = list(set([m[0] for m in courses2 if m[0] is not None]))
        _c = len(set(curso1) & set(curso2))
        _err = "No coinciden los ID de Curso en las tablas Organization + Course + K12Course"
        logger.info(f"Aprobado") if _c == len(curso1) == len(curso2) else logger.error(_err)
      else:
        logger.error(f"S/Datos")

      return True    

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista jerarquiasList para obtener la lista de organizaciones: {str(e)}")
      logger.error(f"Rechazado")
      return False

  def separaRUT(self, *args):
    if (len(args) > 0) and (args[0] is not None):
      dv = ''.join([c for c in list(args[0].upper()) if c.isalpha()])
      aux = ''.join([c for c in list(args[0]) if c.isdigit()])
      if(dv == ''):
        dv = aux[-1:]
        aux = aux[:-1]
    else:
      aux=0
      dv=0
    return aux,dv

  def validarRut(self, aux,dv):
    revertido = map(int, reversed(str(aux)))
    factors = cycle(range(2,8))
    s = sum(d * f for d, f in zip(revertido,factors))
    res = (-s)%11
    if ((str(res) == dv) or (dv=="K" and res==10)):
      return True
    return False

  #VERIFICA SI EL RUT INGRESADO ES VALIDO
  def validarRUN(self, *args):
    aux,dv = self.separaRUT(*args)
    if(aux!=0 and dv!=0):
      if(self.validarRut(aux,dv)):
        if(int(aux)<=47000000):
          return True
    return False

  def validarIpe(self, *args):
    aux,dv = self.separaRUT(*args)
    if(aux!=0 and dv!=0):
      if(self.validarRut(aux,dv)):
        if(int(aux)>=100000000):
          return True
    return False

  def convertirArray2DToList(self, text):
    _l = []
    for e in text:
      if "|" not in str(e):
        _l.append(e)
      else:
        for subE in e.split("|"):
          _l.append(subE)
    return _l

  def imprimeErrores(self, lista,fn,msg):
    _l = self.convertirArray2DToList(lista)  
    _err = set([e for e in _l if not fn(e)])
    _r   = False if len(_err)>0 else True
    _t = f"{msg}: {_r}. {_err}";logger.info(_t);
    return _err,_r

  def validaFormatoE164Telefono(self, e):
    r = re.compile('^\+56\d{9,15}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoNumero(self, e):
    r = re.compile('^\d{0,4}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoFecha(self, e):
    r = re.compile('^([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaTribalAffiliation(self, e):
    _lista = self.listValidations['TribalAffiliationDescriptionList']
    return True if e in _lista else False

  def validaBoolean(self, e):
    return e

  # VERIFICA DATOS DE LAS ORGANIZACIONES
  # VERIFICA JERARQUIA DE LOS DATOS
  # la jerarquí es:
  #  RBD -> Modalidad -> Jornada -> Niveles -> Rama -> 
  #  Sector Económico -> Especialidad ->
  #  Tipo de Curso -> COD_ENSE -> Grado -> Curso -> Asignatura
  def validaFormatoRBD(self, e):
    r = re.compile('^RBD\d{5}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaModalidad(self, e):
    _lista = list(set(self.listValidations['modalidadesList']))
    return True if e in _lista else False

  def validaJornada(self, e):
    _lista =  list(set(self.listValidations['jornadasList']))
    return True if e in _lista else False

  def validaNivel(self, e):
    _lista =  list(set(self.listValidations['nivelList']))
    return True if e in _lista else False

  def validaRama(self, e):
    _lista =  list(set(self.listValidations['ramaList']))
    return True if e in _lista else False

  def validaSector(self, e):
    _lista =  list(set(self.listValidations['sectorList']))
    return True if e in _lista else False

  def validaEspecialidad(self, e):
    _lista =  list(set(self.listValidations['especialidadList']))
    return True if e in _lista else False

  def validaTipoCurso(self, e):
    _lista =  list(set(self.listValidations['tipoCursoList']))
    return True if e in _lista else False

  def validaCodigoEnse(self, e):
    _lista =  list(set(self.listValidations['codigoEnseList']))
    return True if e in _lista else False

  def validaGrado(self, e):
    _lista =  list(set(self.listValidations['gradoList']))
    return True if e in _lista else False

  def validaLetraCurso(self, e):
    r = re.compile('^[A-Z]{1,2}$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaFormatoClaveAleatoria(self, e):
    r = re.compile('^[0-9]{6}+([-]{1}[0-9kK]{1})?$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  # VERIFICA DATOS DE LAS ORGANIZACIONES
  def verificaClaveAleatoriaDocentes(self, conn):
    _r = False;error=''
    rows = conn.execute("SELECT ClaveAleatoriaDocente FROM oprList where ClaveAleatoriaDocente not null;").fetchall()
    logger.info(f"len(ClaveAleatoriaDocente): {len(rows)}")
    if(len(rows)>0):
      # Valida los números de clave aleatoria de los docentes
      data = list(set([m[0] for m in rows if m[0] is not None]))
      _err,_r = self.imprimeErrores(data,self.validaFormatoClaveAleatoria,"VERIFICA FORMATO Clave Aleatoria Docente")
    else:
      _err = "La BD no contiene clave aleatoria de los docentes"
    
    _t = f"VERIFICA CLAVE ALEATORIA DOCENTES: {_r}"
    logger.info(_t);
    return _r

### Appoderado INICIO ###
### Appoderado FIN ###

### WebClass INICIO ###
def fn8F1(self, conn, idIncident):
        try:
            querySelect = "select * from Incident where IncidentId ="
            queryWhere = str(idIncident)
            queryComplete = querySelect+queryWhere
            query = conn.execute(queryComplete).fetchall()
            if(len(query)>0):
                querySelect = "select * from K12StudentDiscipline where IncidentId ="
                queryWhere = str(idIncident)
                queryComplete = querySelect+queryWhere
                try:
                    query = conn.execute(queryComplete).fetchall()
                    if(len(query)>0):
                        query = len(query)
                        print(f'Total de datos: {query}')
                        print(f'Aprobado')
                        return True
                    else:
                        logger.error(f'S/Datos')
                        return False
                except Exception as e:
                    logger.error(f'No se pudo ejecutar la consulta: {str(e)}')
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f'S/Datos')
                return False
        except Exception as e:
            logger.error(f'No se pudo ejecutar la consulta: {str(e)}')
            logger.error(f'Rechazado')
            return False
  
  ############# Funcion fn5E4 WC ###################################
def fn5E4(self,conn):
    try:
        i=0
        conex=conn.cursor()
        asistencia_alumnos=conex.execute("""select P.FirstName,P.LastName,o.OrganizationId,VirtualIndicator,CSL.OrganizationId,RAE.Date,Cr.ClassroomIdentifier,RAE.RefAttendanceStatusId,RAE.RefAbsentAttendanceCategoryId,RAE.RefLeaveEventTypeId,RAE.RefPresentAttendanceCategoryId,RAE.observaciones from Organization o
        join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
        join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
        join Person P on OPR.PersonId = P.PersonId
        join Location L on  L.LocationId = o.OrganizationId
        join Classroom Cr on L.LocationId = Cr.LocationId
        join CourseSectionLocation CSL on CR.LocationId = CSL.LocationId
        join CourseSection CS on CSL.OrganizationId = CS.OrganizationId
    where OPR.RoleId = 6 ; """).fetchall()
        a=len(asistencia_alumnos)
        if i<=a:
            for fila in asistencia_alumnos:
                if fila[8] is None:
                    logger.error(f"No esta registrado el estatus de la asistencia")
                    logger.error(f"Rechazado")
                    return False 
                else:
                    if fila[3] is not None: 
                        if fila[3]==1:
                            logger.info(f"asistencia Virtual")
                        elif fila[3]==0:
                            logger.info(f"asistencia presencial")
                    else:
                        logger.error(f"el campo virtualindicator esta vacio")
                        logger.error(f"Rechazado")
                        return False
                    if fila[7]==1:continue
                    else:
                        if fila[7] ==  2 or 3:
                            if fila[8] is not None:
                                continue
                            else:
                                logger.error(f" descripcion de la ausencia sin datos en la asistencia")
                                logger.error(f"Rechazado")
                                return False
                        elif fila[7] == 4 or 6:
                            if fila[10] is not None:continue
                            else:
                                logger.error(f"descripcion del status sin datos en la asistencia")
                                logger.error(f"Rechazado")
                                return False
                        elif fila[7] == 5:
                            if fila[9] is not None:continue
                            else:
                                logger.error(f"descripcion de la salida temprana sin datos ")
                                logger.error(f"Rechazado")
                                return False
                if i==a:
                    logger.info(f'Verificacion Terminada, todos los alumnos cuentan con registro de asistencia y su estatus')
                    logger.info(f'Validacion Aprobada')
                    return True 
                else: i+=1   
        else:  
            logger.error(f"S/datos")
            logger.error(f"Rechazado")
            return False
    except Exception as e:
          logger.error(f"NO se pudo ejecutar la verificación en la lista")
          logger.error(f"Rechazado")
          return False
  
  ############# Fin fn5E4 WC #######################################
  
  ############# Inicio fn5E5 #######################################
  
def fn5E5(self,conn):
    try:
        i=0
        conex=conn.cursor()
        asistencia_docentes=conex.execute("""select P.FirstName,P.LastName,digitalRandomKey,Date,EntryDate,ExitDate,CSL.OrganizationId,RefAttendanceStatusId, RAE.RefAbsentAttendanceCategoryId,RAE.RefLeaveEventTypeId,RAE.RefPresentAttendanceCategoryId,RAE.observaciones  from Organization o
        join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
        join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
        join Person P on OPR.PersonId = P.PersonId
        join Location L on  L.LocationId = o.OrganizationId
        join Classroom Cr on L.LocationId = Cr.LocationId
        join CourseSectionLocation CSL on CR.LocationId = CSL.LocationId
        join CourseSection CS on CSL.OrganizationId = CS.OrganizationId
        where OPR.RoleId = 5""").fetchall()
        a=len(asistencia_docentes)
        if asistencia_docentes is not None:
            
            for fila in asistencia_docentes:
                if fila[2] is None:
                    logger.error(f"Existen Profesores sin claves digitales")
                    logger.error(f"Rechazado")
                    return False
                else:
                    if fila[3] and fila[4] is not None:
                        if fila[3] != fila[4]:
                                    if fila[3]!=fila[5]:
                                        logger.error(f"profesor cuenta con multiples entradas o salidas en la misma fecha, Verificar")
                                        logger.error(f"Rechazado")
                                        return False
                                    else:
                                        logger.info(f" registro de salida ")
                                        return True
                        else:
                            logger.info(f"registro de entrada")
                            return True
                    else:
                            logger.error(f"Asistencia sin fecha y hora registrada")
                            return False        
        else:
            logger.error(f"S/Datos ")
            logger.error(f"Rechazado")                 
    except Exception as e:
        logger.error(f"NO se pudo ejecutar la verificación en la lista")
        logger.error(f"Rechazado")
        return False
  
  
############# Fin fn5E5 ####################################### 
## fn5D0 WEBCLASS ##
def fn5D0(self, conn, date):
        try:
            formatDate = datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')
            strFormatDate = datetime.strftime(formatDate, '%Y-%m-%d %H:%M:%S:%f') #FULL DATE
            strFormatYMD = datetime.strftime(formatDate, '%Y-%m-%d') #YEATS, MONTH, DAY 
            strFormatHours = datetime.strftime(formatDate, '%H:%M:%S:%f') #HOURS, MINUTES, SECONDS
            querySelect = 'Select * from RoleAttendanceEvent where date like'
            queryDate  = "'"+'%'+strFormatYMD+'%'+"'"
            queryRefAttendanceEventTypeId = ' and RefAttendanceEventTypeId = 2'
            queryComplete = querySelect+queryDate+queryRefAttendanceEventTypeId 
            queryRoleAttendanceEvent = conn.execute(queryComplete).fetchall()

            if (len(queryRoleAttendanceEvent)==1):
                totalData = len(queryRoleAttendanceEvent)
                print(f'Datos totales segun fecha: {totalData}')
                print(f'Aprobado')
                return True
            elif (len(queryRoleAttendanceEvent)>=2):
                sing = list(set([m[8] for m in queryRoleAttendanceEvent if m[8] is not None]))
                totalData = len(queryRoleAttendanceEvent)
                lista = list(set([m[0] for m in queryRoleAttendanceEvent if m[0] is not None]))
                filterId = str(lista)
                filterId = filterId.replace('[','(')
                filterId = filterId.replace(']',')')
                
                querySelect = 'select * from RoleAttendanceEvent where RoleAttendanceEventId in'
                queryAndDate = ' and date > '
                queryDate = "'"+strFormatDate+"'"
                queryComplete = querySelect+filterId+queryAndDate+queryDate
                query = conn.execute(queryComplete).fetchall()
                idDates = list(set([m[0] for m in query if m[0] is not None]))
                dates = list(set([m[2] for m in query if m[2] is not None]))
                singTwo = list(set([m[8] for m in query if m[8] is not None]))
                if(len(dates)>=1):
                    if(sing == singTwo):
                        print(f'Firmas iguales')
                        logger.error(f'Rechazado')
                        return False
                    elif(sing != singTwo):
                        OrganizationPersonRole = list(set([m[1] for m in query if m[1] is not None]))
                        OrganizationPersonRole  = str(OrganizationPersonRole)
                        OrganizationPersonRole = OrganizationPersonRole.replace('[','(')
                        OrganizationPersonRole = OrganizationPersonRole.replace(']',')')
                        queryRole = conn.execute('select * from OrganizationPersonRole where OrganizationPersonRoleId ='+OrganizationPersonRole).fetchall()
                        if(len(queryRole)>0):
                            Role = list(set([m[1] for m in queryRole if m[1] is not None]))
                            Role = str(Role)
                            if(Role == '[1]'):
                                print(f'Fecha cuenta con firma de director')
                                print(f'Aprobado')
                            else:
                                logger.error(f'Rol de persona no autorizado')
                                logger.error(f'Rechazado')
                        else:
                            logger.error(f'S/Datos')
                            logger.error(f'Rechazado')
                else:
                    logger.error(f'S/Datos')
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f'No se pudo ejecutar la consulta: {e}')
            logger.error(f'Rechazado')
            return False
## fin fn5D0 ##

###fn4fa
def fn4fa(self, conn,idorga):
  ##recibe un id de organizacion
    try:
      #trae todos los datos bases del alumno y de su apoderado por curso
          listaAlumno = conn.execute("""
          select op.personid,
          op.organizationpersonroleid ,
          StudentListNumber as "numero de lista",
          Identifier as rut,
          p.Birthdate as "fecha de nacimiento",
          k12se.FirstEntryDateIntoUSSchool,
          rf.Definition as sexo,
          pad.StreetNumberAndName as direccion
          , (p.FirstName||' '||p.MiddleName||' '||p.LastName||' '||p.SecondLastName) as "nombre completo"
          , (p2.FirstName||' '||p2.MiddleName||' '||p2.LastName||' '||p2.SecondLastName) as "apoderado",
          pad2.StreetNumberAndName as "domicilio Apoderado"
          , pt2.TelephoneNumber as "fono apoderado",
          pea2.EmailAddress as "email apoderado"
          , k12SE.FirstEntryDateIntoUSSchool as 'fecha de inicio'
          from K12StudentEnrollment k12SE join OrganizationPersonRole op on (k12SE.OrganizationPersonRoleId = op.OrganizationPersonRoleId)
          join PersonIdentifier PI on op.PersonId = PI.PersonId join person p on op.PersonId = p.PersonId join RefSex rf on p.RefSexId = rf.RefSexId
          join PersonAddress pad on p.PersonId = pad.PersonId join PersonRelationship PR  on p.PersonId = PR.RelatedPersonId join Person p2 on p2.PersonId=pr.PersonId
          join PersonAddress pad2 on pad2.PersonId= p2.PersonId join PersonTelephone pt2 on pt2.PersonId = p2.PersonId
          join PersonEmailAddress pea2  on pea2.PersonId = p2.PersonId
          where op.RoleId = 6 and length(Identifier)  > 5 and op.organizationid = ?
          group by op.PersonId;
          """,([idorga])).fetchall()
            
          if(len(listaAlumno)> 0):
              for alumno in listaAlumno:
                  #por cada alumno trae a los profesorees que interactuan con el
                  idAlumno=alumno[0]
                  idAlumnorole=alumno[1]
                  listaProfesionales = conn.execute("""select (p.FirstName||' '||p.MiddleName||' '||p.LastName||' '||p.SecondLastName) as "nombre completo"
                  from OrganizationPersonRole op join OrganizationPersonRole op2 on op.OrganizationId=op2.OrganizationId
                  join Person p on op.PersonId = p.PersonId
                  where op.roleid !=6 and op2.PersonId= ?
                  group by op.PersonId;""",([idAlumno])).fetchall()
                  #lista de becas e identificacion de estudiante preferente, prioritario, etc de ser requerido
                  listaPrograma = conn.execute("""
                  select description from RefParticipationType rpt join PersonProgramParticipation ppp on rpt.RefParticipationTypeId = ppp.RefParticipationTypeId
                  where ppp.OrganizationPersonRoleId = ?;""",([idAlumnorole])).fetchall()
                  #trae las asignaturas en las que se encuentra el alumno
                  organizacion = conn.execute("""
                  select op.OrganizationId,personid from OrganizationPersonRole op join Organization o on op.OrganizationId = o.OrganizationId
                  where personid in ( ?) ;""",([idAlumno])).fetchall()
                  organi=[]
                  evalua=[]
                   
                  for org in organizacion:
                      #por cada asignatura trae el calendario
                      calendario = conn.execute("""select BeginDate,EndDate,SessionStartTime,SessionEndTime
                              from calendarList
                              where OrganizationId = ? and "RefSessionType.Description" like '%Full School Year%';
                            """,([org[0]])).fetchall()
                      if(calendario):
                          organi.append(calendario)
                        # por cada asignatura trae las evaluaciones
                      evaluaciones = conn.execute(""" select name,begindate,Description from calendarList where OrganizationId=? and Description like '%Evaluacion%';""",([org[0]])).fetchall()
                      if (evaluaciones):
                          evalua.append(evaluaciones)
                
                 
              becasprogramas=(list([m[0] for m in listaPrograma if m[0] is not None]))
              evalua=(list([m[0] for m in evaluaciones if m[0] is not None]))
              profe=(list([m[0] for m in listaProfesionales if m[0] is not None]))
              calenda=(list([m[0] for m in calendario if m[0] is not None]))
                
              if not profe:
                  logger.error(f"Sin profesores")
                  return False
              elif not evalua:
                  logger.error(f"Sin evaluaciones")
                  return False
              elif not calenda:
                  logger.error(f"Sin calendario")
                  return False
              elif not becasprogramas:
                  logger.error(f"Sin becas o estudiante preferente")
                  return False
                
          else:
              logger.error(f"Sin Datos de alumnos")
              return False

          logger.info("Se validaron todos los datos")
          logger.info(f"Aprobado")
          return True

    except Exception as e:
          logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
          logger.error(f"Rechazado")
          return False
#####
#fn5f0

def fn5f0(self, conn,fecha,organizacion):
    try:
        alumnosPresentes = conn.execute("""select count(rae.date),opr.organizationid,  strftime('%d', `Date`) as 'dia',strftime('%m', `Date`) as 'mes', date as hora from RoleAttendanceEvent rae
        join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
        where refattendancestatusid = 1 and date = ? and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
        group by  rae.date , opr.OrganizationId order  by rae.date;""",([fecha,organizacion])).fetchall()

        alumnosAusentes = conn.execute("""select count(rae.date),opr.organizationid from RoleAttendanceEvent rae
        join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
        where refattendancestatusid in (2,3) and date = ? and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
        group by  rae.date , opr.OrganizationId order  by rae.date;""",([fecha,organizacion])).fetchall()

        alumnosRetrasados = conn.execute("""select count(rae.date) from RoleAttendanceEvent rae
        join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
        where refattendancestatusid = 4 and date = ? and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
        group by  rae.date , opr.OrganizationId order  by rae.date;""",([fecha,organizacion])).fetchall()


        profesorObservacion = conn.execute("""select Identifier,observaciones,rae.OrganizationPersonRoleId,opr.OrganizationId from PersonIdentifier PI
        join OrganizationPersonRole OPR on PI.PersonId=OPR.PersonId
        join RoleAttendanceEvent RAE on Rae.OrganizationPersonRoleId=opr.OrganizationPersonRoleId
        where  opr.RoleId !=6 and date = ? and opr.organizationid = ?;""",([fecha,organizacion])).fetchall()

        fecha2=(list([m[4] for m in alumnosPresentes if m[4] is not None]))


        asignatura = conn.execute("""select ClassPeriod,name from CourseSectionSchedule css join Organization o on o.OrganizationId=css.OrganizationId
        where  ? between ClassBeginningTime and ClassEndingTime;""",(fecha2)).fetchall()

        aPresentes=(list([m[0] for m in alumnosPresentes if m[0] is not None]))
        seccion=(list([m[1] for m in alumnosPresentes if m[1] is not None]))
        dia=(list([m[2] for m in alumnosPresentes if m[2] is not None]))
        mes=(list([m[3] for m in alumnosPresentes if m[3] is not None]))
        periodo=(list([m[0] for m in asignatura if m[0] is not None]))
        name=(list([m[1] for m in asignatura if m[1] is not None]))
            
        if not alumnosAusentes :
              aAusentes=0
        else:
              aAusentes=alumnosAusentes[0][0]

        if not alumnosRetrasados :
              aRetrasados=0
        else:
                
              aRetrasados=alumnosRetrasados[0]

        if not profesorObservacion :
              logger.error(f"Sin profesores")
              return False
        if not dia:
              logger.error(f"Sin dia")
              return False
        if not mes:
              logger.error(f"Sin mes")
              return False
        if not seccion:
              logger.error(f"Sin seccion")
              return False
        if not aPresentes:
              logger.error(f"Sin presentes")
              return False
        if not name:
              logger.error(f"Sin asignatura")
              return False
        if not periodo:
              logger.error(f"Sin periodo")
              return False
            
              
        return True

    except Exception as e:
          logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
          logger.error(f"Rechazado")
          return False

## INICIO fn8F3##
def fn8F3(self, conn ):
        queryTwo = conn.execute('select * from k12StudentDiscipline where refDisciplinaryActionTakenId = 8').fetchall()
        if(len(queryTwo)>0):
            for fila in queryTwo:
                for dato in fila:
                    if dato:
                        a = 0
                    else:
                        logger.error(f'Datos incompletos')
                        logger.error(f'Rechazado')
                        return False
        else:
            logger.error(f'S/Datos')
            logger.error(f'Rechazado')
            return False 
        return True
## Fin fn8F3##
## Fin fn8F2##
def fn8F2(self,conn):
    try:
        i=0
        j=0
        conex=conn.cursor()
        Lista_incidencias=conex.execute(""" from * Organization O join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
    join Person P on OPR.PersonId = P.PersonId
    join Incident I on OPR.OrganizationPersonRoleId = I.OrganizationPersonRoleId
    join IncidentPerson IP on I.IncidentId = IP.IncidentId
    join K12StudentDiscipline KSD on I.IncidentId = KSD.IncidentId
    where OPR.RoleId=6;""").fetchall()
        a=len(Lista_incidencias)
        d=len(Lista_incidencias[0])
        if Lista_incidencias is not None:
            
            for fila in Lista_incidencias:
                for fila[i] in fila:
                    if fila[i] is None:
                        logger.error(f"Dato en vacion en incidencia:",{fila},"no se puede continuar validacion", )
                        return False
                    else:
                        if j==a and i==d:
                            logger.info(f"Datos completos y verificados")
                            return True
                        else:
                            j+=1
                            i+=1 
        else:
            logger.error(f"S/Datos ")
            logger.error(f"Rechazado")                 
        
    except Exception as e:
        logger.error(f"NO se pudo ejecutar la verificación en la lista")
        logger.error(f"Rechazado")
        return False
        
## Fin fn8F2##

### WebClass FIN ###



### Registro de Salidas y Retiros (No Habituales) Mi Aula INICIO ###
### Registro de Salidas y Retiros (No Habituales) Mi Aula FIN ###
