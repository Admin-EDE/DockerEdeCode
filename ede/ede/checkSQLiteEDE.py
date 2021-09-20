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
    self.args = args
    logger.info(f"tipo de argumento: {type(self.args)}, valores: {self.args}");
    self.functions = {
      "fn0FA": "self.fn0FA(conn)",
      "fn0FB": "self.fn0FB(conn)",
      "fn1FA": "self.fn1FA(conn)",
      "fn1FB": "self.fn1FB(conn)",
      "fn1FC": "self.fn1FC(conn)",
      "fn2FA": "self.fn2FA(conn)",
      "fn2EA": "self.fn2EA(conn)",
      "fn2DA": "self.fn2DA(conn)",
      "fn2DB": "self.fn2DB(conn)",
      "fn2CA": "self.fn2CA(conn)",
      "fn2CB": "self.fn2CB(conn)",
      "fn2BA": "self.fn2BA(conn)",
      "fn2AA": "self.fn2AA(conn)",
      "fn29A": "No/Verificado",
      "fn29B": "No/Verificado",
      "fn29C": "No/Verificado",
      "fn28A": "self.fn28A(conn)",
      "fn28B": "self.fn28B(conn)",
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
      "fn5F0": "self.fn5f0(conn,fecha,organizacion)",
      "fn5E0": "No/Verificado",
      "fn5E1": "self.fn5E1(conn)",
      "fn5E2": "No/Verificado",
      "fn5E3": "No/Verificado",
      "fn5E4": "self.fn5E4(conn)",
      "fn5E5": "self.fn5E5(conn)",
      "fn5E6": "No/Verificado",
      "fn5D0": "self.fn5D0(conn date)",
      "fn6F0": "self.fn6F0(conn)",
      "fn6F1": "self.fn6F1(conn)",
      "fn6E0": "self.fn6E0(conn)",
      "fn6E1": "No/Verificado",
      "fn6E2": "self.fn6E2(conn)",
      "fn6E3": "No/Verificado",
      "fn6E4": "No/Verificado",
      "fn6E5": "No/Verificado",
      "fn6D0": "self.fn6D0(conn)",
      "fn6D1": "self.fn6D1(conn)",
      "fn6C0": "self.fn6C0(conn)",
      "fn6C1": "No/Verificado",
      "fn6C2": "self.fn6C2(conn)",
      "fn6B0": "No/Verificado",
      "fn6B1": "No/Verificado",
      "fn6A0": "No/Verificado",
      "fn6A1": "No/Verificado",
      "fn6A2": "No/Verificado",
      "fn6A3": "No/Verificado",
      "fn690": "No/Verificado",
      "fn680": "No/Verificado",
      "fn681": "self.fn681(conn)",
      "fn682": "No/Verificado",
      "fn7F0": "No/Verificado",
      "fn7F1": "No/Verificado",
      "fn7F2": "No/Verificado",
      "fn7F3": "No/Verificado",
      "fn7F4": "No/Verificado",
      "fn7F5": "No/Verificado",
      "fn7F6": "No/Verificado",
      "fn8F0": "No/Verificado",
      "fn8F1": "self.fn8F1(conn)",
      "fn8F2": "self.fn8F2(conn)",
      "fn8F3": "self.fn8F3(conn)",
      "fn9F0": "No/Verificado",
      "fn9F1": "No/Verificado",
      "fn9F2": "self.fn9F2(conn)",
      "fn9F3": "self.fn9F3(conn)"
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
        _t = f"VERIFICACION DEL FORMATO DEL NUMERO DE LISTA DE LAS PERSONAS: {_r}. {_err}"
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
        _t = f"VERIFICACION DEL NUMERO DE MATRICULA DE LAS PERSONAS: {_r}. {_err}"
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
        _t = f"VERIFICACION DEL FORMATO DE LAS DE LAS PERSONAS: {_r}. {_err}"
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
        _t = f"Valida que la cantidad de #matricula == #Lista == #fechasIncorporaciónes: {_r}.  NumLista:{len(_l1)}, NumMat:{len(_l2)}, FechaIncorporación:{len(_l3)}";
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
      rows = conn.execute("SELECT RBD,nombreEstablecimiento,modalidad,jornada,nivel,rama,sector,especialidad,tipoCurso,codigoEnseñanza,grado,letraCurso  FROM jerarquiasList;").fetchall()
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
    r = re.compile('^((19|20)(\d{2})-(1[0-2]|0?[0-9])-([12][0-9]|3[01]|0?[1-9]))[ T]?((0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(.\d{0,})?)?([+-](0[0-9]|1[0-9]|2[0-3]):([0-5][0-9]))?$')
    if(isinstance(e,str)):
      return r.match(e) is not None
    return False

  def validaTribalAffiliation(self, e):
    _lista = list(set(self.listValidations['TribalAffiliationDescriptionList']))
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

## WebClass INICIO ##
## Inicio fn2FA WC ##
  def fn2FA(self, conn):
            try:
                results = conn.execute("""
                select count(distinct PersonId)-(select count(distinct PersonId) from OrganizationPersonRole
                where RoleId=6
                and ExitDate is not null)
                from OrganizationPersonRole
                where  EntryDate is not null and RoleId=6  ;
                """).fetchall()

                resultsTwo = conn.execute("""
                select count(distinct K12StudentEnrollment.OrganizationPersonRoleId)
                from K12StudentEnrollment
                where RefEnrollmentStatusId is not null
                AND FirstEntryDateIntoUSSchool IS NOT NULL;
                """).fetchall()

                if(len(results)>0 and len(resultsTwo)>0):
                    lista = list(set([m[0] for m in results if m[0] is not None]))
                    listaDos = list(set([m[0] for m in resultsTwo if m[0] is not None]))
                    if lista == listaDos:
                        logger.info(f"La cantidad de matriculados corresponder con los alumnos inscritos")
                        logger.info(f"Aprobado")
                        return True
                    else :
                        logger.error(f'La cantidad de alumnos matriculados no cocincide con los inscritos')
                        logger.error(f'Rechazado')
                        return False
                else:
                    logger.error(f"S/Datos")
                    logger.error(f'No hay registros de matriculas')
                    return False
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return False
## Fin fn2FA WC ##

##Inicio fn2EA WC ##
  def fn2EA(self, conn):
        try:
            results = conn.execute("""
            select (select identifier  from PersonIdentifier pi
            join RefPersonIdentificationSystem rfi on pi.RefPersonIdentificationSystemId=rfi.RefPersonIdentificationSystemId
            where Code like '%School%' and pi.PersonId=p.PersonId) as "matricula"
            ,(select identifier from PersonIdentifier pi
            join RefPersonIdentificationSystem rfi on pi.RefPersonIdentificationSystemId=rfi.RefPersonIdentificationSystemId
            where (Code like '%IPE%' or Code like '%RUN%')and pi.PersonId=p.PersonId) as "cedula"
            , p.FirstName as "primer nombre"
            , p.MiddleName as "otros nombres"
            , p.LastName as "apellidoPaterno"
            , p.SecondLastName as "apellidoMaterno"
            , case when RTA.Description is null then 'ninguna' else RTA.Description
                end as "tribalAffiationDescription"
            , Role.Name as rol
            , rf.Description as sexo
            , p.Birthdate as "fechaCumpleaños"
            , opr.EntryDate as "fecha de incorporacion"
            ,RefCountry.Description as pais
            ,rfs.Description as region
            ,pa.City
            ,rfc.Description as comuna
            ,pa.AddressCountyName
            ,pa.StreetNumberAndName as direccion
            ,pa.ApartmentRoomOrSuiteNumber
            ,pa.PostalCode
            , p2.FirstName as "Nombre Apoderado"
            , p2.MiddleName as "segundo nombre apoderado"
            , p2.LastName as "apellidoPaterno apoderado"
            , p2.SecondLastName as "apellidoMaterno apoderado"
            , RefCountry2.Description as paisApoderado
            , rfs2.Description as regionApoderado
            , pa2.City as ciudadapoderado
            ,rfc2.Description as comunaApoderado
            ,pa2.AddressCountyName
            ,pa2.StreetNumberAndName as direccionApoderado
            ,pa2.ApartmentRoomOrSuiteNumber
            ,pa2.PostalCode as codigoPostalApoderado
            ,rfpiv.Description
            ,pt2.TelephoneNumber as numeroTelefonicoApoderado
            ,rfptnt.Description as tipoNumeroApoderado
            ,pt2.PrimaryTelephoneNumberIndicator
            ,pea2.EmailAddress as emailApoderado
            ,rfet.Description as tipoEmail
            ,opr.ExitDate as fechaRetiro
            ,opr.OrganizationId

            from Person p join RefSex rf on p.RefSexId = rf.RefSexId
            join OrganizationPersonRole opr on opr.PersonId=p.PersonId
            left join RefTribalAffiliation RTA on p.RefTribalAffiliationId = RTA.RefTribalAffiliationId
            left join Role on Role.RoleId=opr.RoleId
            join PersonAddress pa on pa.PersonId=p.PersonId
            left join RefCountry on pa.RefCountryId = RefCountry.RefCountryId
            left join RefState rfs on pa.RefStateId= rfs.RefStateId
            left join RefCounty rfc on pa.RefCountyId = rfc.RefCountyId
            left join PersonRelationship prs on p.PersonId=prs.RelatedPersonId
            left join Person p2 on p2.PersonId=prs.PersonId
            left join PersonAddress pa2 on pa2.PersonId=p2.PersonId
            left join RefCountry RefCountry2 on pa.RefCountryId = RefCountry2.RefCountryId
            left join RefState rfs2 on pa2.RefStateId= rfs2.RefStateId
            left join RefCounty rfc2 on pa2.RefCountyId = rfc2.RefCountyId
            left join RefPersonalInformationVerification rfpiv on pa2.RefPersonalInformationVerificationId = rfpiv.RefPersonalInformationVerificationId
            left join PersonTelephone pt2 on pt2.PersonId = p2.PersonId
            left join RefPersonTelephoneNumberType rfptnt on pt2.RefPersonTelephoneNumberTypeId = rfptnt.RefPersonTelephoneNumberTypeId
            left join PersonEmailAddress pea2 on p2.PersonId=pea2.PersonId
            left join RefEmailType rfet on rfet.RefEmailTypeId = pea2.PersonEmailAddressId
            join Organization o on o.OrganizationId=opr.OrganizationId

            where opr.RoleId=6 and o.RefOrganizationTypeId=21
            group by  p.PersonId;
            """).fetchall()

            for fila in results:
                seccion=fila[38]
                nivel = conn.execute("""select  Nivel.Name
                    from Organization Nivel
                    join RefOrganizationType rft on Nivel.RefOrganizationTypeId = rft.RefOrganizationTypeId
                    join OrganizationRelationship or1 on or1.OrganizationId=Nivel.OrganizationId
                    join OrganizationRelationship or2 on or1.OrganizationId=or2.Parent_OrganizationId
                    join OrganizationRelationship or3 on or2.OrganizationId=or3.Parent_OrganizationId
                    join OrganizationRelationship or4 on or3.OrganizationId=or4.Parent_OrganizationId
                    join OrganizationRelationship or5 on or4.OrganizationId=or5.Parent_OrganizationId
                    join OrganizationRelationship or6 on or5.OrganizationId=or6.Parent_OrganizationId
                    join OrganizationRelationship or7 on or6.OrganizationId = or7.Parent_OrganizationId
                    join OrganizationRelationship or8 on or7.OrganizationId = or8.Parent_OrganizationId
                    where or8.OrganizationId= ?;""",([seccion])).fetchall()
                cursos = conn.execute("""select asi.name from Organization asi join OrganizationRelationship ors on ors.OrganizationId=asi.OrganizationId
                    where ors.Parent_OrganizationId = ?;""",([seccion])).fetchall()
                if (fila[0] is None):
                    logger.error(f"alumno sin matricula")
                    logger.error(f"Rechazado")
                    return False
                if (fila[1] is None):
                    logger.error(f"alumno sin identificacion")
                    logger.error(f"Rechazado")
                    return False
                if (fila[2] is None ):
                    logger.error(f"alumno sin nombre")
                    logger.error(f"Rechazado")
                    return False
                if (fila[4] is None):
                    logger.error(f"alumno sin apellido paterno")
                    logger.error(f"Rechazado")
                    return False
                if (fila[5] is None):
                    logger.error(f"alumno sin apellido materno")
                    logger.error(f"Rechazado")
                    return False
                if (fila[6] is None):
                    logger.error(f"alumno sin tribalAffillation")
                    logger.error(f"Rechazado")
                    return False
                if (fila[7] is None):
                    logger.error(f"alumno sin rol")
                    logger.error(f"Rechazado")
                    return False
                if (fila[8] is None):
                    logger.error(f"alumno sin sexo")
                    logger.error(f"Rechazado")
                    return False
                if (fila[9] is None):
                    logger.error(f"alumno sin fecha cumpleaños")
                    logger.error(f"Rechazado")
                    return False
                if (fila[10] is None):
                    logger.error(f"alumno sin fecha de entrada")
                    logger.error(f"Rechazado")
                    return False
                if (fila[11] is None):
                    logger.error(f"alumno sin pais")
                    logger.error(f"Rechazado")
                    return False
                if (fila[12] is None):
                    logger.error(f"alumno sin region")
                    logger.error(f"Rechazado")
                    return False
                if (fila[13] is None):
                    logger.error(f"alumno sin ciudad")
                    logger.error(f"Rechazado")
                    return False
                if (fila[14] is None):
                    logger.error(f"alumno sin comuna")
                    logger.error(f"Rechazado")
                    return False
                if (fila[13] is None):
                    logger.error(f"alumno sin sector")
                    logger.error(f"Rechazado")
                    return False
                if (fila[14] is None):
                    logger.error(f"alumno sin direccion")
                    logger.error(f"Rechazado")
                    return False
                if (fila[16] is None):
                    logger.error(f"alumno sin codigo postal")
                    logger.error(f"Rechazado")
                    return False
                if (fila[17] is None):
                    logger.error(f"apoderado alumno sin nombre")
                    logger.error(f"Rechazado")
                    return False
                if (fila[19] is None):
                    logger.error(f"apoderado alumno sin apellido paterno")
                    logger.error(f"Rechazado")
                    return False
                if (fila[20] is None):
                    logger.error(f"apoderado alumno sin apellido materno")
                    logger.error(f"Rechazado")
                    return False
                if (fila[21] is None):
                    logger.error(f"apoderado alumno sin pais")
                    logger.error(f"Rechazado")
                    return False
                if (fila[22] is None):
                    logger.error(f"apoderado alumno sin region")
                    logger.error(f"Rechazado")
                    return False
                if (fila[23] is None):
                    logger.error(f"apoderado alumno sin ciudad")
                    logger.error(f"Rechazado")
                    return False
                if (fila[24] is None):
                    logger.error(f"apoderado alumno sin comuma")
                    logger.error(f"Rechazado")
                    return False
                if (fila[25] is None):
                    logger.error(f"apoderado alumno sin sector")
                    logger.error(f"Rechazado")
                    return False
                if (fila[26] is None):
                    logger.error(f"apoderado alumno sin direccion")
                    logger.error(f"Rechazado")
                    return False
                if (fila[28] is None):
                    logger.error(f"apoderado alumno sin codigo postal")
                    logger.error(f"Rechazado")
                    return False
                if (fila[29] is None):
                    logger.error(f"apoderado alumno sin tipo de documento para acreditar domicilio")
                    logger.error(f"Rechazado")
                    return False
                if (fila[30] is None):
                    logger.error(f"apoderado alumno sin numero telefonico")
                    logger.error(f"Rechazado")
                    return False
                if (fila[31] is None):
                    logger.error(f"apoderado alumno sin tipo de numero telefonico")
                    logger.error(f"Rechazado")
                    return False
                if (fila[32] is None):
                    logger.error(f"apoderado alumno sin verificador de numero primario")
                    logger.error(f"Rechazado")
                    return False
                if (fila[33] is None):
                    logger.error(f"apoderado alumno sin email")
                    logger.error(f"Rechazado")
                    return False
                if (fila[34] is None):
                    logger.error(f"apoderado alumno sin tipo de email")
                    logger.error(f"Rechazado")
                    return False
                if (nivel is None):
                    logger.error(f"alumno sin nivel")
                    logger.error(f"Rechazado")
                    return False
                if (cursos is None):
                    logger.error(f"alumno sin asignaturas")
                    logger.error(f"Rechazado")
                    return False
            logger.info(f"datos de alumnos validados")
            logger.error(f"Aprobado")
            return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"S/DATOS")
            return False
## Fin fn2EA WC ##

## Inicio fn2DA WC ##
  def fn2DA(self,conn):
        try:
            _query = conn.execute("""
            select
                DISTINCT
                PS.PersonStatusId,
                P.FirstName,
                P.LastName,
                P.MiddleName,
                P.SecondLastName,
                PS.StatusValue,
                trim(PS.fileScanBase64),
                PS.docNumber
            from OrganizationPersonRole OPR
            join Person P on OPR.PersonId = P.PersonId
            join PersonStatus PS on P.PersonId = PS.PersonId
            where
            OPR.RoleId = 6
            and PS.RefPersonStatusTypeId = 27
            """).fetchall()
            if(len(_query)>0):
                _registros=(list([m[6] for m in _query  if m[6] is not None]))
                _contador = 0
                for x in _registros:
                    x = x.upper()
                    if(x == 'PROMOCION' or x == 'MATRICULA' or x == 'TRANSLADO'):
                        _contador += 1
                if(len(_registros) == _contador):
                    logger.info(f'Todos los alumnos poseen de matricula definitiva')
                    logger.info(f'Aprobado')
                    return True
                else:
                    logger.error(f'No todos los alumnos poseen documento de matricula definitiva')
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'No existen alumnos con matricula definitiva')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn2DA WC ##

## Inicio fn2DB WC ##
  def fn2DB(self,conn):
        try:
            _query = conn.execute("""
            select DISTINCT P.PersonId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join PersonStatus PS on P.PersonId = PS.PersonId
            where OPR.RoleId = 6
              and PS.RefPersonStatusTypeId = 33;
            """).fetchall()
            if(len(_query)>0):
              _queryType = conn.execute("""
              SELECT PS.PersonId
              FROM PersonStatus PS
              WHERE PS.PersonId in (select DISTINCT P.PersonId
                                    from OrganizationPersonRole OPR
                                            join Person P on OPR.PersonId = P.PersonId
                                            join PersonStatus PS on P.PersonId = PS.PersonId
                                    where OPR.RoleId = 6
                                      and PS.RefPersonStatusTypeId = 33)
                AND PS.fileScanBase64 IS NOT NULL
                AND PS.fileScanBase64 <> '';
              """).fetchall
              if(len(_query) == len(_queryType)):
                logger.info(f'Todos los alumnos matriculados bajo el decreto 152 poseen su documento correspondiente')
                logger.info(f'Aprobado')
                return True
              else:
                logger.error(f'No existe documento para los alumnos matriculados bajo el decreto 152')
                logger.error(f'Rechazado')
                return False
            else:
                logger.error(f"S/Datos")
                logger.error(f"No existen alumnos matriculados bajo el decreto 152, artículo 60")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn2DB WC ##

## Inicio fn2CA WC ##
  def fn2CA(self,conn):
        try:
            _query = conn.execute("""
            select
            DISTINCT
                P.FirstName,
                P.LastName,
                P.MiddleName,
                P.SecondLastName,
                PS.StatusStartDate,
                PS.StatusEndDate,
                PS.Description,
                PS.StatusValue,
                PS.fileScanBase64,
                PS.docNumber
            from OrganizationPersonRole OPR
            join Person P on OPR.PersonId = P.PersonId
            join PersonStatus PS on P.PersonId = PS.PersonId
            where
                OPR.RoleId = 6
                and PS.RefPersonStatusTypeId = 30
            """).fetchall()
            if(len(_query)>0):
                _statusStartDate =  list(set([m[4] for m in _query if m[1] is not None]))
                _statusEndDate =  list(set([m[5] for m in _query if m[1] is not None]))
                _fileScanBase64 =  list(set([m[8] for m in _query if m[8] is not None]))
                contador = 0
                for rowStart in _statusStartDate:
                    for rowEnd in _statusEndDate:
                        for file in _fileScanBase64:
                            if rowEnd >= rowStart:
                                file = file.upper()
                                if (file == "MOTIVO" or file == "IDENTIFICADOR"):
                                    contador += 1
                            else:
                                logger.error(f'Las fechas de retiro son menores a las de entrada al establecimiento del alumno')
                                logger.error(f'Rechazado')
                                return False
                if (contador == len(_fileScanBase64)):
                    logger.info(f'')
                    logger.info(f'Aprobado')
                    return True
            else:
                logger.error(f'S/Datos')
                logger.error(f'No existen registros de alumnos retirados del establecimiento')
                return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn2CA WC ##

 ## Inicio fn2CB WC ##
  def fn2CB(self,conn):
        try:
            _query = conn.execute("""
            select OPR.OrganizationPersonRoleId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join PersonStatus PS on P.PersonId = PS.PersonId
            where OPR.RoleId = 6
              and PS.RefPersonStatusTypeId = 30;
            """).fetchall()
            if(len(_query)>0):
                _queryEntregaDocumentos = conn.execute("""
                select I.IncidentId
                from Incident I
                        join IncidentPerson IP on I.IncidentId = IP.IncidentId
                where I.RefIncidentBehaviorId = 33
                  and I.OrganizationPersonRoleId in (select OPR.OrganizationPersonRoleId
                                                    from OrganizationPersonRole OPR
                                                              join Person P on OPR.PersonId = P.PersonId
                                                              join PersonStatus PS on P.PersonId = PS.PersonId
                                                    where OPR.RoleId = 6
                                                      and PS.RefPersonStatusTypeId = 30);
                """).fetchall()
                if(len(_query)==len(_queryEntregaDocumentos)):
                  logger.info(f'Todos los alumnos retirados del establecimiento cuentan con una entrega de documentos respectiva al apoderado')
                  logger.info(f'Aprobado')
                  return True
                else:
                  logger.error(f'Los alumnos retirados del establecimiento no cuentan con un registro de entrega de documentos al apoderado')
                  logger.error(f'Rechazado')
                  return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'No existen registro de alumnos retirados del establecimiento')
                return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
 ## Fin fn2CB WC ##
  
 ## Inicio fn2BA WC ##
  def fn2BA(self,conn):
        try:
            _query = conn.execute("""
            select DISTINCT P.PersonId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join PersonStatus PS on P.PersonId = PS.PersonId
            where OPR.RoleId = 6
              and PS.RefPersonStatusTypeId IN (25, 24, 31);
            """).fetchall()
            if (len(_query)>0):
              _queryExcedentes = conn.execute("""
              select 'file' as fileScanBase64
              from PersonStatus
              where fileScanBase64 is not null
                and fileScanBase64 <> ''
                and PersonId in (
                  select DISTINCT P.PersonId
                  from OrganizationPersonRole OPR
                          join Person P on OPR.PersonId = P.PersonId
                          join PersonStatus PS on P.PersonId = PS.PersonId
                  where OPR.RoleId = 6
                    and PS.RefPersonStatusTypeId IN (25, 24, 31));
              """).fetchall()
              if (len(_queryExcedentes) == len(_query)):
                logger.info(f'Todos los alumnos excedentes cuentan con su documento correspondiente')
                logger.info(f'Aprobado')
                return True
              else:
                logger.error(f'Los alumnos excedentes no cuentan con su documento correspondiente')
                logger.error(f'Rechazado')
                return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'No existen alumnos excedentes en el establecimiento')
                return True
        except Exception as e:
            logger.error(f'NO se pudo ejecutar la verificación en la lista')
            logger.error(f'Rechazado')
            return False
  ## Fin fn2BA WC ##
  
## Inicio fn29A WC ##
  def fn29A(self, conn):
        try:
            results = conn.execute("""
            select opr.PersonId,
                  (select o2.OrganizationId
                    from OrganizationPersonRole opr2
                            join Organization o2 on o2.OrganizationId = opr2.OrganizationId
                    where RefOrganizationTypeId = 21
                      and opr2.PersonId = opr.PersonId)                              as seccion,

                  (select opr3.OrganizationPersonRoleId
                    from OrganizationPersonRole opr3
                    where opr3.PersonId = opr.PersonId)                              as personrole,

                  (select grado.OrganizationId
                    from Organization grado
                            join RefOrganizationType rft on grado.RefOrganizationTypeId = rft.RefOrganizationTypeId
                            join OrganizationRelationship or1 on or1.OrganizationId = grado.OrganizationId
                            join OrganizationRelationship or2 on or1.OrganizationId = or2.Parent_OrganizationId
                    where or2.OrganizationId = (select o2.OrganizationId
                                                from OrganizationPersonRole opr2
                                                        join Organization o2 on o2.OrganizationId = opr2.OrganizationId
                                                where o2.RefOrganizationTypeId = 21
                                                  and opr2.PersonId = opr.PersonId)) as grado,
                  o3.Name
            from OrganizationPersonRole opr
                    join Organization o on o.OrganizationId = opr.OrganizationId
                    join K12StudentEnrollment k12se on k12se.OrganizationPersonRoleId = personrole
                    join Organization o3 on o3.OrganizationId = grado
                    join PersonStatus ps on opr.PersonId = ps.PersonId
            where o.RefOrganizationTypeId = 47 /*cambiar a id respectivo, este id hace referencia a el nuevo tipo de organizacion agregado para practicaProfesional*/
              and k12se.RefEnrollmentStatusId = 2
              and cast(strftime('%Y', k12se.FirstEntryDateIntoUSSchool) as integer) =
                  cast(strftime('%Y', current_timestamp) as integer)
              and ps.RefPersonStatusTypeId = 26;
            """).fetchall()
            if(len(results)>0):
                lista = list(set([m[0] for m in results if m[0] is not None]))
                lista2 = list(set([m[4] for m in results if m[4] is not None]))
                con=len(lista)
                x=0
                for l1 in lista:
                    grado=(lista2[x][4])
                    if (grado[-8:-1].lower()=="3° medio"):
                        results2 = conn.execute("""
                        select opr.organizationid
                        from OrganizationPersonRole opr
                                join K12StudentCourseSection k12cs on opr.OrganizationPersonRoleId = k12cs.OrganizationPersonRoleId
                                join Organization o on o.OrganizationId = opr.OrganizationId
                        where PersonId = ?
                          and k12cs.RefCourseSectionEnrollmentStatusTypeId = 6
                          and cast(strftime('%Y', opr.EntryDate) as integer) = cast(strftime('%Y', current_timestamp) as integer);
                        """([l1])).fetchall()
                        if (len(results2))<1:
                            logger.error(f"alumno en practica  de 3 año sin requisito de semestre cumplido")
                            logger.error(f"Rechazado")
                            return False
                    x+=x
                logger.info(f"todos los alumnos de practica cumplen con los requisitos")
                logger.info(f"Aprobado")
                return True

            else:
                logger.error(f"S/Datos")
                logger.error(f"No existen alumnos en practica registrados")
                return True

        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn29A WC ##

## Inicio fn29B WC ##
  def fn29B(self, conn):
        try:
            query = conn.execute("""
            select OPR.OrganizationId, P.PersonId, count(P.PersonId)
            from Person P
                    join OrganizationPersonRole OPR on P.PersonId = OPR.PersonId
                    join PersonStatus PS on P.PersonId = PS.PersonId
            where RoleId = 6
              and ps.RefPersonStatusTypeId = 35
              and OrganizationPersonRoleId in (select OrganizationId
                                              from Organization
                                              where RefOrganizationTypeId = 47)
            group by OPR.OrganizationId, P.PersonId;
            """).fetchall()
            k12StudentEnrollment = conn.execute("""
            select OrganizationPersonRoleId
            from K12StudentEnrollment;
            """).fetchall()
            if(len(query)>0 and len(k12StudentEnrollment)>0):
                estudiantes = (list([m[2] for m in query if m[2] is not None]))
                organizaciones = (list([m[0] for m in query if m[0] is not None]))
                organizacionesK12 = (list([m[0] for m in k12StudentEnrollment if m[0] is not None]))
                for x in estudiantes:
                    if(x == 2):
                        logger.error(f"Matriculas repetidas")
                        logger.error(f"Rechazado")
                        return False
                    else:
                        for y in organizacionesK12:
                            for z in organizaciones:
                                if(y in z):
                                    contador = contador + 1
                                else:
                                    logger.error(f"Matricula/s no registrada/s")
                                    logger.error(f"Rechazado")
                                    return False
                logger.info(f'Matriculas ingresadas correctamente')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"S/Datos")
                logger.error(f"No existen alumnos en practica registrados")
                return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn29B WC ##
## Inicio fn29C WC ##
  def fn29C(self, conn):
        try:
            _queryStud = conn.execute("""
            select OPR.OrganizationId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join Organization O on OPR.OrganizationId = O.OrganizationId
            where OPR.RoleId = 6
              and O.RefOrganizationTypeId = 47
            group by P.PersonId,
                    OPR.OrganizationId;
            """).fetchall()

            _queryProf = conn.execute("""
            select OPR.OrganizationId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join Organization O on OPR.OrganizationId = O.OrganizationId
            where OPR.RoleId = 17
              and O.RefOrganizationTypeId = 47
            group by P.PersonId,
                    OPR.OrganizationId;
            """).fetchall()
            if((len(_queryStud)>0) and (len(_queryProf)>0)):
                _organizationStu = (list([m[5] for m in _queryStud if m[0] is not None]))
                if not _organizationStu :
                    logger.error(f"Sin Alumnos")
                    logger.error(f'Rechazado')
                    return False
                _organizationProf = (list([m[5] for m in _queryProf if m[0] is not None]))
                if not _organizationProf :
                    logger.error(f"Sin profesores")
                    logger.error(f'Rechazado')
                    return False
                _contador = 0
                z = len(_organizationStu)
                for x in _organizationStu:
                    for y in _organizationProf:
                        if x in y:
                            _contador += 1
                if _contador == z:
                    logger.info(f'Todos los alumnos en practica con profesor')
                    logger.info(f'Aprobado')
                    return True
                else:
                    logger.error(f'Alumnos en practica sin profesor')
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f"S/Datos")
                logger.error(f"No existen alumnos en practica registrados")
                return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn29C WC ##

## Inicio fn5E0 WC ##
  def fn5E0(self, conn):
        try:
            organizations = conn.execute("""
            select
                RAE.date,
                OPR.OrganizationId
            from Organization O
                    join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
                    join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
            where O.RefOrganizationTypeId = 22
            order by RAE.Date asc;
            """).fetchall()
            if(len(organizations)>0):
                dates = list(set([m[0] for m in organizations if m[0] is not None]))
                organizationsId = list(set([m[1] for m in organizations if m[1] is not None]))
                for d in dates:
                    for o in organizationsId:
                        try:
                            d = d[0]
                            o = o
                            alumnosPresentes = conn.execute("""
                            select count(rae.date),
                                  opr.organizationid,
                                  date as hora
                            from RoleAttendanceEvent rae
                                    join OrganizationPersonRole opr on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                            where refattendancestatusid = 1
                              and date = ?
                              and opr.organizationid = ?
                              and rae.OrganizationPersonRoleId in
                                  (select opr2.OrganizationPersonRoleId
                                  from OrganizationPersonRole opr2
                                  where RoleId = 6)
                            group by rae.date, opr.OrganizationId
                            order by rae.date;
                            """,([str(d),str(o)])).fetchall()

                            alumnosAusentes = conn.execute("""
                            select count(rae.date),
                                  opr.organizationid
                            from RoleAttendanceEvent rae
                                    join OrganizationPersonRole opr on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                            where refattendancestatusid in (2, 3)
                              and date = ?
                              and opr.organizationid = ?
                              and rae.OrganizationPersonRoleId in
                                  (select opr2.OrganizationPersonRoleId
                                  from OrganizationPersonRole opr2
                                  where RoleId = 6)
                            group by rae.date, opr.OrganizationId
                            order by rae.date;
                            """,([str(d),str(o)])).fetchall()

                            alumnosRetrasados = conn.execute("""
                            select count(rae.date)
                            from RoleAttendanceEvent rae
                                    join OrganizationPersonRole opr on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                            where refattendancestatusid = 4
                              and date = ?
                              and opr.organizationid = ?
                              and rae.OrganizationPersonRoleId in
                                  (select opr2.OrganizationPersonRoleId
                                  from OrganizationPersonRole opr2
                                  where RoleId = 6)
                            group by rae.date, opr.OrganizationId
                            order by rae.date;
                            """,([str(d),str(o)])).fetchall()

                            alumnosTotales = conn.execute("""
                            select count(rae.date)
                            from RoleAttendanceEvent rae
                                    join OrganizationPersonRole opr on rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                            where date = ?
                              and opr.organizationid = ?
                              and rae.OrganizationPersonRoleId in
                                  (select opr2.OrganizationPersonRoleId
                                  from OrganizationPersonRole opr2
                                  where RoleId = 6)
                            group by rae.date, opr.OrganizationId
                            order by rae.date;
                            """,([str(d),str(o)])).fetchall()

                            profesorObservacion = conn.execute("""
                            select Identifier,
                                  observaciones,
                                  RAE.OrganizationPersonRoleId,
                                  OPR.OrganizationId,
                                  RAE.VirtualIndicator
                            from PersonIdentifier PI
                                    join OrganizationPersonRole OPR on PI.PersonId = OPR.PersonId
                                    join RoleAttendanceEvent RAE on RAE.OrganizationPersonRoleId = OPR.OrganizationPersonRoleId
                            where RAE.VirtualIndicator != 0
                              and OPR.RoleId != 6
                              and date = ?
                              and OPR.organizationid = ?;
                            """,([str(d),str(o)])).fetchall()
                            fecha2=(list([m[4] for m in alumnosPresentes if m[4] is not None]))
                            if (len(fecha2)<=0):
                                logger.error(f'Sin firmas')
                                logger.error(f'Rechazado')
                                return False
                            asignatura = conn.execute("""
                            select ClassPeriod,
                                  name,
                                  ClassMeetingDays
                            from CourseSectionSchedule css
                                    join Organization o on o.OrganizationId = css.OrganizationId
                            where ? between ClassBeginningTime and ClassEndingTime;
                            """,(fecha2)).fetchall()

                            firma = (list([m[4] for m in profesorObservacion if m[4]is not None]))

                            aPresentes=(list([m[0] for m in alumnosPresentes if m[0] is not None]))
                            aAusentes=(list([m[0] for m in alumnosAusentes if m[0] is not None]))
                            aRetrasados=(list([m[0] for m in alumnosRetrasados if m[0] is not None]))
                            aTotales=(list([m[0] for m in alumnosTotales if m[0] is not None]))

                            seccion=(list([m[1] for m in alumnosPresentes if m[1] is not None]))
                            periodo=(list([m[0] for m in asignatura if m[0] is not None]))
                            name=(list([m[1] for m in asignatura if m[1] is not None]))

                            if (int(alumnosPresentes) + int(alumnosAusentes) + int(alumnosRetrasados)) != int(alumnosTotales):
                                logger.error(f'Asistencia incorrecta')
                                logger.error(f'Rechazado')
                                return False

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
                                logger.error(f'Rechazado')
                                return False
                            if not seccion:
                                logger.error(f"Sin seccion")
                                logger.error(f'Rechazado')
                                return False
                            if not aPresentes:
                                logger.error(f"Sin presentes")
                                logger.error(f'Rechazado')
                                return False
                            if not name:
                                logger.error(f"Sin asignatura")
                                logger.error(f'Rechazado')
                                return False
                            if not periodo:
                                logger.error(f"Sin periodo")
                                logger.error(f'Rechazado')
                                return False
                            return True
                        except Exception as e:
                            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                            logger.error(f"Rechazado")
                            return False
                logger.info(f'Aprobado')
                logger.info(f'Clases validadas')
                return True
            else:
                logger.error(f"S/Datos")
                logger.error(f'Sin asistencia por bloque')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn5E0 WC ##

## Inicio fn8F1 WC ##
  def fn8F1(self, conn):
          try:
              query = conn.execute("""select * from Incident""").fetchall()
              if(len(query)>0):
                  Incidentes = (list([m[0] for m in query if m[0] is not None]))
                  for x in Incidentes:
                      querySelect = "select * from K12StudentDiscipline where IncidentId = "
                      queryWhere = str(x)
                      queryComplete = querySelect+queryWhere
                      try:
                          query = conn.execute(queryComplete).fetchall()
                          if(len(query)>0):
                              query = len(query)
                              logger.info(f'Total de datos: {query}')
                              logger.info(f'Aprobado')
                              return True
                          else:
                              logger.error(f'S/Datos')
                              logger.error(f'Rechazado')
                              return False
                      except Exception as e:
                          logger.error(f'No se pudo ejecutar la consulta: {str(e)}')
                          logger.error(f'Rechazado')
                          return False
              else:
                  logger.error(f'S/Datos')
                  logger.error(f'Rechazado')
                  return True
          except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F1 WC ##

## Inicio fn5E4 WC ##
  def fn5E4(self,conn):
      try:
          i=0
          asistencia_alumnos=conn.execute("""select P.FirstName,P.LastName,o.OrganizationId,RAE.VirtualIndicator,CSL.OrganizationId,RAE.Date,Cr.ClassroomIdentifier,RAE.RefAttendanceStatusId,RAE.RefAbsentAttendanceCategoryId,RAE.RefLeaveEventTypeId,RAE.RefPresentAttendanceCategoryId,RAE.observaciones from Organization o
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
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn5E4 WC ##
## Inicio fn5E5 WC ##
  def fn5E5(self,conn):
        try:
            _query = conn.execute("""
            select
                RAE.digitalRandomKey,
                RAE.Date
            from Organization O
                    join OrganizationPersonRole OPR on O.OrganizationId = OPR.OrganizationId
                    join RoleAttendanceEvent RAE on OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
            where O.RefOrganizationTypeId = 22
            and OPR.RoleId in (4, 5)
            and RAE.Date between(RAE.Date||'10:00:00') and (RAE.Date||'11:45:00');
            """).fetchall()
            _lenQuery = int(len(_query))
            _contador = 0
            if(len(_query)>0):
                _digitalRandomKey = (list([m[0] for m in _query if m[0] is not None]))
                if not _digitalRandomKey:
                    logger.error(f"Sin firmas registradas")
                    logger.error(f'Rechazado')
                    return False
                for x in _digitalRandomKey:
                    if x is None:
                        logger.error(f'Sin firmas registradas')
                        logger.error(f'Rechazado')
                        return False
                    if (len(x)>0):
                        _contador += 1
                if(_lenQuery == _contador):
                    logger.info(f'Firmas registradas correctamente')
                    logger.info(f'Aprobado')
                    return True
            else:
                logger.error(f'S/Datos')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn5E5 WC ##

## Inicio fn5D0 WC ##
  def fn5D0(self, conn):
        try:
            _oPR = conn.execute("""
                SELECT DISTINCT count(RAE.Date), OPR.PersonId, RAE.Date, RAE.digitalRandomKey,RAE.VirtualIndicator
                FROM OrganizationPersonRole OPR
                        JOIN RoleAttendanceEvent RAE ON OPR.OrganizationPersonRoleId = RAE.OrganizationPersonRoleId
                WHERE OPR.RoleId in(4,5)
                AND RAE.RefAttendanceEventTypeId = 2
                group by OPR.PersonId, RAE.Date, RAE.digitalRandomKey, RAE.VirtualIndicator;
                """
                ).fetchall()
            if(len(_oPR)>0):
                _count = (list([m[0] for m in _oPR if m[0] is not None]))
                _contador = 0
                for x in _count:
                    if(x > 1):
                        _contador += 1
                if(_contador > 0):
                    logger.error('Duplicados')
                    logger.error('Rechazado')
                    return False
                else:
                    logger.info('No hay duplicados')
                    logger.info('Aprobado')
                    return True
            else:
                logger.error(f'No existen materias')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## fin fn5D0 WC ##

## Inicio fn4FA WC ##
  def fn4FA(self, conn):
            try:
            #trae todos los datos bases del alumno y de su apoderado por curso
                idorga = conn.execute("""
                SELECT
                    organizationId,
                    name
                from Organization
                where RefOrganizationTypeId = 21
                """).fetchall()
                x=0
                for org in idorga:
                    print(org)
                    listaAlumno = conn.execute("""
                    SELECT op.personid,
                        op.organizationpersonroleid,
                        StudentListNumber                                                                        as "numero de lista",
                        Identifier                                                                               as rut,
                        p.Birthdate                                                                              as "fecha de nacimiento",
                        k12SE.FirstEntryDateIntoUSSchool,
                        rf.Definition                                                                            as sexo,
                        pad.StreetNumberAndName                                                                  as direccion
                            ,
                        (p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName)     as "nombre completo"
                            ,
                        (p2.FirstName || ' ' || p2.MiddleName || ' ' || p2.LastName || ' ' || p2.SecondLastName) as "apoderado",
                        pad2.StreetNumberAndName                                                                 as "domicilio Apoderado"
                            ,
                        pt2.TelephoneNumber                                                                      as "fono apoderado",
                        pea2.EmailAddress                                                                        as "email apoderado"
                            ,
                        k12SE.FirstEntryDateIntoUSSchool                                                         as "fecha de inicio"
                        from K12StudentEnrollment k12SE
                            join OrganizationPersonRole op on (k12SE.OrganizationPersonRoleId = op.OrganizationPersonRoleId)
                            join PersonIdentifier PI on op.PersonId = PI.PersonId
                            join Person p on op.PersonId = p.PersonId
                            join refsex rf on p.RefSexId = rf.RefSexId
                            left join PersonAddress pad on p.PersonId = pad.PersonId
                            left join PersonRelationship pr on p.PersonId = pr.RelatedPersonId
                            left join Person p2 on p2.PersonId = pr.PersonId
                            left join PersonAddress pad2 on pad2.PersonId = p2.PersonId
                            left join PersonTelephone pt2 on pt2.PersonId = p2.PersonId
                            left join PersonEmailAddress pea2 on pea2.PersonId = p2.PersonId
                    where op.RoleId = 6
                    and length(Identifier) > 5
                    and op.organizationid = ?
                    group by op.PersonId;
                    """,([org[0]])).fetchall()
                    if(len(listaAlumno)> 0):
                        x=x+1
                        for alumno in listaAlumno:
                            #por cada alumno trae a los profesorees que interactuan con el
                            idAlumno=alumno[0]
                            idAlumnorole=alumno[1]
                            listaProfesionales = conn.execute("""
                            select (p.FirstName || ' ' || p.MiddleName || ' ' || p.LastName || ' ' || p.SecondLastName) as "nombre completo"
                            from OrganizationPersonRole op
                                    join OrganizationPersonRole op2 on op.OrganizationId = op2.OrganizationId
                                    join Person p on op.PersonId = p.PersonId
                            where op.roleid != 6
                            and op2.PersonId = ?
                            and op2.OrganizationID = ?
                            group by op.PersonId
                            """,(idAlumno,org[0])).fetchall()
                            #lista de becas e identificacion de estudiante preferente, prioritario, etc de ser requerido
                            listaPrograma = conn.execute("""
                            select rpt.description
                            from refparticipationtype rpt
                                    join PersonProgramParticipation ppp on rpt.RefParticipationTypeId = ppp.RefParticipationTypeId
                            where ppp.OrganizationPersonRoleId = ?;
                            """,([idAlumnorole])).fetchall()
                            #trae las asignaturas en las que se encuentra el alumno
                            organizacion = conn.execute("""
                            select op.OrganizationId, personid
                            from OrganizationPersonRole op
                                    join Organization o on op.OrganizationId = o.OrganizationId
                            where personid in (?);""",([idAlumno])).fetchall()
                            organi=[]
                            evalua=[]
                            for org in organizacion:
                                #por cada asignatura trae el calendario
                                calendario = conn.execute("""
                                select
                                    BeginDate,
                                    EndDate,
                                    SessionStartTime,
                                    SessionEndTime
                                from calendarList
                                where OrganizationId = ?
                                and "RefSessionType.Description" like '%Semester%';
                                    """,([org[0]])).fetchall()
                                if(calendario):
                                    organi.append(calendario)
                                # por cada asignatura trae las evaluaciones
                                evaluaciones = conn.execute(""" select name,begindate,CalendarDescription from calendarList where OrganizationId=? and CalendarDescription like '%Evaluacion%';""",([org[0]])).fetchall()
                                if (evaluaciones):
                                    evalua.append(evaluaciones)
                        becasprogramas=(list([m[0] for m in listaPrograma if m[0] is not None]))
                        evalua=(list([m[0] for m in evaluaciones if m[0] is not None]))
                        profe=(list([m[0] for m in listaProfesionales if m[0] is not None]))
                        calenda=(list([m[0] for m in calendario if m[0] is not None]))
                        print(profe)
                        if not profe:
                            logger.error(f"Sin profesores")
                            logger.error(f"Rechazado")
                            return False
                        elif not evalua:
                            logger.error(f"Sin evaluaciones")
                            logger.error(f"Rechazado")
                            return False
                        elif not calenda:
                            logger.error(f"Sin calendario")
                            logger.error(f"Rechazado")
                            return False
                    else:
                        logger.error(f"Sin Datos de alumnos: {org}")
                if x==0:
                  logger.error(f"Sin Datos de alumnos")
                  logger.error(f"Rechazado")
                  return False
                logger.info("Se validaron todos los datos")
                logger.info(f"Aprobado")
                return True
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return False
## Fin fn4FA WC ##
## Inicio fn5F0 WC ##
  def fn5F0(self, conn):
        try:
            idorga= conn.execute("""
                select organizationId from organization where RefOrganizationTypeId = 22
            """).fetchall()
            for org in idorga:
                alumnosPresentes = conn.execute("""select count(rae.date),opr.organizationid,  strftime('%d', `Date`) as 'dia',strftime('%m', `Date`) as 'mes', date as hora from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid = 1  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                alumnosAusentes = conn.execute("""select count(rae.date),opr.organizationid from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid in (2,3)  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                alumnosRetrasados = conn.execute("""select count(rae.date) from RoleAttendanceEvent rae
                join OrganizationPersonRole opr on Rae.OrganizationPersonRoleId = opr.OrganizationPersonRoleId
                where refattendancestatusid = 4  and opr.organizationid = ? and
                                        rae.OrganizationPersonRoleId in
                                        (select opr2.OrganizationPersonRoleId
                                        from OrganizationPersonRole opr2
                                        where RoleId = 6  )
                group by  rae.date , opr.OrganizationId order  by rae.date;""",([org[0]])).fetchall()
                profesorObservacion = conn.execute("""select Identifier,observaciones,rae.OrganizationPersonRoleId,opr.OrganizationId from PersonIdentifier PI
                join OrganizationPersonRole OPR on PI.PersonId=OPR.PersonId
                join RoleAttendanceEvent RAE on Rae.OrganizationPersonRoleId=opr.OrganizationPersonRoleId
                where  opr.RoleId !=6  and opr.organizationid = ?;""",([org[0]])).fetchall()
                if (alumnosPresentes):
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
                        logger.error(f"Rechazado")
                        return False
                    if not dia:
                        logger.error(f"Sin dia")
                        logger.error(f"Rechazado")
                        return False
                    if not mes:
                        logger.error(f"Sin mes")
                        logger.error(f"Rechazado")
                        return False
                    if not seccion:
                        logger.error(f"Sin seccion")
                        logger.error(f"Rechazado")
                        return False
                    if not aPresentes:
                        logger.error(f"Sin presentes")
                        logger.error(f"Rechazado")
                        return False
                    if not name:
                        logger.error(f"Sin asignatura")
                        logger.error(f"Rechazado")
                        return False
                    if not periodo:
                        logger.error(f"Sin periodo")
                        logger.error(f"Rechazado")
                        return False
                else:
                    logger.error(f"S/Datos")
                    return True
            logger.info(f'Validacion aprobada')
            logger.info(f'Aprobado')
            return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
  ## Fin fn5F0 WC ##

## Inicio fn8F3 WC ##
  def fn8F3(self, conn):
        try:
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
              return True
          logger.info(f'Datos Validados')
          logger.info(f'Aprobado')
          return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F3 WC ##

## Inicio fn8F2 WC ##
  def fn8F2(self,conn):
        try:
            _queryIncident = conn.execute("""
            select DISTINCT
                I.IncidentId,
                I.IncidentDate,
                I.IncidentTime,
                I.RefIncidentTimeDescriptionCodeId,
                I.IncidentDescription,
                I.OrganizationPersonRoleId,
                OPR.RoleId
                from Incident I
                join OrganizationPersonRole OPR on I.OrganizationPersonRoleId = OPR.OrganizationPersonRoleId;
                """).fetchall()
            _lenQuery = len(_queryIncident)
            if(len(_queryIncident)>0):
                _incidentId = (list([m[0] for m in _queryIncident if m[0] is not None]))
                if not _incidentId:
                    logger.error(f"Sin Incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentDate = (list([m[1] for m in _queryIncident if m[1] is not None]))
                if not _incidentDate:
                    logger.error(f"Sin Fecha registrada para los incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentTime = (list([m[2] for m in _queryIncident if m[2] is not None]))
                if not _incidentTime:
                    logger.error(f"Sin Time registrada para los incidentes")
                    logger.error(f'Rechazado')
                    return False
                _incidentTimeDescriptionCodeId = (list([m[3] for m in _queryIncident if m[3] is not None]))
                if not _incidentTimeDescriptionCodeId:
                    logger.error(f"Sin time code en el incidente")
                    logger.error(f'Rechazado')
                    return False
                _incidentDescription = (list([m[4] for m in _queryIncident if m[4] is not None]))
                if not _incidentDescription:
                    logger.error(f"Sin descripcion para el incidente")
                    logger.error(f'Rechazado')
                    return False
                _organizationPersonRoleId = (list([m[5] for m in _queryIncident if m[5] is not None]))
                if not _organizationPersonRoleId:
                    logger.error(f"Sin estudiante en incidente")
                    logger.error(f'Rechazado')
                    return False
                _roleId = (list([m[6] for m in _queryIncident if m[6] is not None]))
                if _roleId != 6:
                    logger.error(f"Sin estudiante en incidente")
                    logger.error(f'Rechazado')
                    return False
                for x in _incidentId:
                    _queryIncidentPerson = conn.execute("""
                    select DISTINCT
                    I.IncidentId,
                    I.PersonId,
                    I.RefIncidentPersonRoleTypeId,
                    OPR.RoleId
                    from IncidentPerson I
                            join Person P on I.PersonId = P.PersonId
                            join OrganizationPersonRole OPR on P.PersonId = OPR.PersonId
                    where I.IncidentId =
                    """,int(x)).fetchall()
                    if (len(_queryIncidentPerson)>0):
                        _personId = (list([m[1] for m in _queryIncidentPerson if m[1] is not None]))
                        _refRoleType = (list([m[2] for m in _queryIncidentPerson if m[2] is not None]))
                        _role = (list([m[3] for m in _queryIncidentPerson if m[3] is not None]))
                        if not _personId:
                            logger.error(f"Sin personas registradas para el incidente")
                            logger.error(f'Rechazado')
                            return False
                        _profe = 0 #4,5
                        _apoderado = 0 #15
                        _entrevistado = 0  #5
                        _entrevistador = 0 #6
                        for y in _refRoleType:
                            for z in _role:
                                if y == 5:
                                    _entrevistado += 1
                                if y == 6:
                                    _entrevistador += 1
                                if z == 4 or z == 5:
                                    _profe += 1
                                if z == 15:
                                    _apoderado += 1
                        if _entrevistado == 0:
                            logger.error(f'Sin entrevistado en reunion de incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _entrevistador == 0:
                            logger.error(f'Sin entrevistador en reunion de incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _profe == 0:
                            logger.error(f'Sin profesor asignado a incidente')
                            logger.error(f'Rechazado')
                            return False
                        if _apoderado == 0:
                            logger.error(f'Sin apoderado en incidente')
                            logger.error(f'Rechazado')
                            return False
                    else:
                        logger.error(f"Sin personas registradas para el incidente")
                        logger.error(f"Rechazado")
                        return False
                logger.info(f'Incidentes validados')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"S/Datos ")
                logger.error(f"Rechazado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn8F2 WC ##

## Inicio fn2AA WC ##
  def fn2AA(self, conn):
        try:
            results = conn.execute("""
            select p.personId
            from Person p join PersonStatus ps on p.PersonId=ps.PersonId
            where p.RefVisaTypeId=6 and ps.StatusValue=1 and ps.RefPersonStatusTypeId=25;
            """).fetchall()

            resultsTwo = conn.execute("""
            select p.personId
            from Person p
            join PersonStatus ps on p.PersonId=ps.PersonId
            where p.RefVisaTypeId=6 and ps.RefPersonStatusTypeId=25;
            """).fetchall()

            if(len(results)>0 and len(resultsTwo)>0):
                lista = list(set([m[0] for m in results if m[0] is not None]))
                listaDos = list(set([m[0] for m in resultsTwo if m[0] is not None]))


                if lista == listaDos:
                    logger.info(f"todos los alumnos de intercambios fueron aprobados")
                    logger.info(f"Aprobado")
                    return True
                else :
                    logger.error(f'No todos los alumnos de intercambio han sido aprobados')
                    logger.error(f'Rechazado')
                    return False


            else:
                logger.error(f"Sin Datos")
                logger.error(f'Rechazado')
                return False

        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn2AA WC ##

## Inicio fn28A WC ##
  def fn28A(self, conn):
            try:
                _query = conn.execute("""
                select DISTINCT P.PersonId
                from OrganizationPersonRole OPR
                        join Person P on OPR.PersonId = P.PersonId
                        join PersonIdentifier PI on P.PersonId = PI.PersonId
                where PI.RefPersonIdentificationSystemId = 52
                  and OPR.RoleId = 6
                  and PI.Identifier is not null;
                """).fetchall()
                if(len(_query)>0):
                  _personStatus = conn.execute("""
                  select 'file' as fileScanBase64
                  from PersonStatus
                  where RefPersonStatusTypeId = 34
                    and fileScanBase64 is not null
                    and fileScanBase64 <> ''
                    and PersonId in (
                      select DISTINCT P.PersonId
                      from OrganizationPersonRole OPR
                              join Person P on OPR.PersonId = P.PersonId
                              join PersonIdentifier PI on P.PersonId = PI.PersonId
                      where PI.RefPersonIdentificationSystemId = 52
                        and OPR.RoleId = 6
                        and PI.Identifier is not null)
                  """).fetchall()
                  if(len(_query) == len(_personStatus)):
                    logger.info(f'Los alumnos extranjeros cuentan con documento de convalidacion de estudios')
                    logger.info(f'Aprobado')
                    return True
                  else:
                    logger.error(f'No todos los alumnos extranjeros no poseen documento de convalidacion de estudios')
                    logger.error(f'Rechazado')
                    return False
                else:
                    logger.error(f"S/Datos")
                    logger.error(f"No existen estudiantes migrantes registrados en el establecimiento")
                    return True
            except Exception as e:
                logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                logger.error(f"Rechazado")
                return False
## Fin fn28A WC ##

## Inicio fn5E1 WC ##
  def fn5E1(self,conn):
        try:
            _query = conn.execute("""
            select OPR.OrganizationPersonRoleId,
                (SELECT count(OPR.PersonId)
                    from OrganizationPersonRole OPR
                            join Organization O on OPR.OrganizationId = O.OrganizationId
                            join Course C on O.OrganizationId = C.OrganizationId
                    where OPR.RoleId = 6
                    and O.RefOrganizationTypeId = 21) as MatriculasTotales
            from OrganizationPersonRole OPR
                    join Organization O on OPR.OrganizationId = O.OrganizationId
                    join Course C on O.OrganizationId = C.OrganizationId
            where OPR.RoleId = 6
            and O.RefOrganizationTypeId = 21
            group by OPR.OrganizationPersonRoleId;
            """).fetchall()
            if(len(_query)>0):
                _alumnos = (list([m[0] for m in _query if m[0] is not None]))
                if not _alumnos :
                    logger.error(f"Sin alumnos registrados")
                    logger.error(f'Rechazado')
                    return False
                _matriculasTotales = (list([m[1] for m in _query if m[1] is not None]))
                if not _matriculasTotales :
                    logger.error(f"Sin matriculas registradas")
                    logger.error(f'Rechazado')
                    return False
                _totalAlumnos = int(len(_alumnos))
                if int(_matriculasTotales[0]) == _totalAlumnos:
                        _queryRegistroAsistencia = conn.execute("""
                        select DISTINCT RoleAttendanceEventId,
                                        Date,
                                        RefAttendanceEventTypeId
                        from RoleAttendanceEvent
                        where RefAttendanceEventTypeId = 1 and Date is not null
                        group by date;
                        """).fetchall()
                        if(len(_queryRegistroAsistencia)>0):
                            logger.info(f'Matriculas registradas y asistencia diaria realizada')
                            logger.info(f'Aprobado')
                            return True
                        else:
                            logger.error(f'Asistencia diaria no realizada por el establecimiento')
                            logger.error(f'Rechazado')
                            return False
                else:
                    logger.error(f"Sin matriculas no coinciden con total de alumnos registrados")
                    logger.error(f'Rechazado')
                    return False
            else:
                logger.error(f'S/Datos')
                logger.error(f'Rechazado')
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn531 WC ##

## Inicio fn28B WC ##
  def fn28B(self, conn):
        try:
            _query = conn.execute("""
            select DISTINCT PI.PersonId
            from OrganizationPersonRole OPR
                    join Person P on OPR.PersonId = P.PersonId
                    join PersonIdentifier PI on P.PersonId = PI.PersonId
            where PI.RefPersonIdentificationSystemId = 52
              and OPR.RoleId = 6
              and PI.Identifier is not null;
            """).fetchall()
            if(len(_query)>0):
              _queryDocuments = conn.execute("""
              SELECT PS.PersonId
              FROM PersonStatus PS
              WHERE PS.PersonId in (select DISTINCT PI.PersonId
                                    from OrganizationPersonRole OPR
                                            join Person P on OPR.PersonId = P.PersonId
                                            join PersonIdentifier PI on P.PersonId = PI.PersonId
                                    where PI.RefPersonIdentificationSystemId = 52
                                      and OPR.RoleId = 6
                                      and PI.Identifier is not null)
                AND PS.fileScanBase64 IS NOT NULL
                AND PS.fileScanBase64 <> ''
                AND PS.docNumber IS NOT NULL
                AND PS.docNumber <> ''
                AND PS.Description IS NOT NULL
                AND PS.Description <> '';
              """).fetchall()
              if (len(_query) == len(_queryDocuments)):
                logger.info(f'Todos los estudiantes migrantes poseen sus documentos de convalidacion de ramos ingresados correctamente')
                logger.info(f'Aprobado')
                return True
              else:
                logger.error(f'Existen alumnos migrantes con documentos de convalidacion de ramos incompletos')
                logger.error(f'Rechazado')
                return False
            else:
                logger.error(f"No existen estudiantes migrantes registrados en el establecimiento")
                logger.error(f"S/Datos")
                return True
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn28B WC ##

## Fin fn9F2 WC ##
  def fn9F2(self, conn):
        try:
            queryEstudiantes = conn.execute("""
                select
          DISTINCT
          o.OrganizationId,
          o.Name
                from person p join OrganizationPersonRole opr
                on p.PersonId = opr.PersonId
                join Organization O on opr.OrganizationId = O.OrganizationId
                where opr.RoleId = 6 and O.RefOrganizationTypeId = 21;""").fetchall()

            if (len(queryEstudiantes)>0):
                organizations = (list([m[0] for m in queryEstudiantes if m[0] is not None]))
                organizations = str(organizations)
                organizations = organizations.replace('[','(')
                organizations = organizations.replace(']',')')
                querySelect = "select CourseId from CourseSection where CourseId in"
                queryComplete = querySelect+organizations
                try:
                    queryAsignaturas = conn.execute(queryComplete).fetchall()
                    if (len(queryAsignaturas)>0):
                        cursos = (list([m[0] for m in queryAsignaturas if m[0] is not None]))
                        cursos = str(cursos)
                        cursos = cursos.replace('[','(')
                        cursos = cursos.replace(']',')')
                        querySelectCalendar = "select * from OrganizationCalendar where OrganizationId in"
                        queryCalendarComplete = querySelectCalendar+cursos
                        try:
                            queryCalendarios = conn.execute(queryCalendarComplete).fetchall()
                            if (len(queryCalendarios)>0):
                                organizationId = (list([m[1] for m in queryCalendarios if m[0] is not None]))
                                calendarCode = (list([m[2] for m in queryCalendarios if m[0] is not None]))
                                calendarDescripction = (list([m[3] for m in queryCalendarios if m[0] is not None]))
                                calendarYear = (list([m[4] for m in queryCalendarios if m[0] is not None]))

                                if not organizationId :
                                    logger.error(f"Sin OrganizationId")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarCode :
                                    logger.error(f"Sin CalendarCode")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarDescripction :
                                    logger.error(f"Sin CaldendarDescription")
                                    logger.error(f'Rechazado')
                                    return False
                                if not calendarYear :
                                    logger.error(f"Sin CalendarYear")
                                    logger.error(f'Rechazado')
                                    return False
                                print(f'Calendarios ingresados correctamente')
                                print(f'Aprobado')
                                return True
                            else:
                                logger.error(f"S/Datos")
                                logger.error(f"Rechazado")
                                return False
                        except Exception as e:
                            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                            logger.error(f"Rechazado")
                            return False
                    else:
                        logger.error(f"S/Datos")
                        logger.error(f"Rechazado")
                        return False
                except Exception as e:
                    logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                    logger.error(f"Rechazado")
                    return False
            else:
                logger.error(f"S/Datos")
                logger.error(f"Rechazado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn9F2 WC ##

## Inicio fn9F3 WC ##
  def fn9F3(self, conn):
        try:
            incident = conn.execute("""
            select IncidentId from Incident;
            """).fetchall()
            if(len(incident)>0):
                listIncident = (list([m[0] for m in incident if m[0] is not None]))
                for x in listIncident:
                    try:
                        x = str(x)
                        incidentParent = conn.execute("""
                        select * from IncidentPerson where IncidentId =
                        """+x+"""
                        where RefIncidentPersonRoleTypeId = 8
                        and RefIncidentPersonTypeId = 43
                        """).fetchall()
                        incidentProfessor = conn.execute("""
                        select * from IncidentPerson where IncidentId =
                        """+x+"""
                        where RefIncidentPersonRoleTypeId = 7
                        and RefIncidentPersonTypeId = 44
                        """).fetchall()
                        parent = 0
                        professor = 0
                        if (len(incidentParent)>0):
                            parent +=1
                        else:
                            logger.error(f"S/Datos")
                            logger.error(f"Rechazado")
                            return False
                        if (len(incidentProfessor)>0):
                            professor += 1
                        else:
                            logger.error(f"S/Datos")
                            logger.error(f"Rechazado")
                            return False
                    except Exception as e:
                        logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
                        logger.error(f"Rechazado")
                        return False
                logger.info(f'Reuniones validas')
                logger.info(f'Aprobado')
                return True
            else:
                logger.error(f"Sin incidentes registrados")
                logger.error(f"Aprobado")
                return False
        except Exception as e:
            logger.error(f"No se pudo ejecutar la consulta: {str(e)}")
            logger.error(f"Rechazado")
            return False
## Fin fn9F3 WC ##

### WebClass FIN ###


### MIAULA INICIO ###

### inicio FN0FA ###
  def fn0FA(self, conn):
    try:
      r_ = conn.execute("""
      SELECT A.personId
      FROM PersonList A
      JOIN OrganizationPersonRole B
      ON A.personId = B.personId
      JOIN jerarquiasList C
      ON B.OrganizationId = C.OrganizationIdDelCurso
      WHERE A.Role like '%Estudiante%'
      AND C.nivel NOT IN ('03:Educación Básica Adultos'
                          ,'06:Educación Media Humanístico Científica Adultos'
                          ,'08:Educación Media Técnico Profesional y Artística, Adultos');
    """).fetchall()

      logger.info(f"VERIFICA QUE EXISTA LISTADO DE ALUMNOS EN VISTA PERSONLIST Y QUE TENGAN PERSONAS ASOCIADAS Y AUTORIZADAS PARA RETIRO.")

      if(len(r_)>0):
        c_ = 0
        p_ = self.convertirArray2DToList(list([m[0] for m in r_ if m[0] is not None]))

        for a_ in p_:
          s_ = "SELECT RetirarEstudianteIndicador  FROM  PersonRelationship WHERE personId = '%s'" %(str(a_))
          t_ = conn.execute(s_).fetchall()
          r2_ = self.convertirArray2DToList(list([m[0] for m in t_ if m[0] is not None]))

          if(len(r2_)>0):
            for r in r2_:
              if(r == True):
                c_ = c_ + 1

        logger.info(f"Total Alumnos                                     : {len(r_)}")
        logger.info(f"Total Personas asociadas y autorizadas para retiro: {c_}")

        if(c_ == len(r_)):
          logger.info(f"TODOS los alumnos tienen informacion de personas asociadas y/o autorizadas para retiro.")
          logger.info(f"Apobado")
          return True
        else:
          logger.error(f"No todos los alumnos tienen informacion de personas asociadas y/o autorizadas para retiro.")
          logger.error(f"Rechazado")
          return False

      else:
        logger.info(f"S/Datos")
        return  False

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta a la vista personList filtrada por alumnos: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin FN0FA ###

### inicio FN0FB ###
  def fn0FB(self, conn):
    try:

      _msg = ""
      _f1 = False
      _drk = 0 #DigitalRandomKey
      _pr = 0 #PersonId autorizada para retiro

      _s1 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType D
                ON B.RefOrganizationTypeId = D.RefOrganizationTypeId
                WHERE a.RoleId = 6
                AND a.ExitDate IS NOT NULL
                AND B.name NOT IN ('03:Educación Básica Adultos'
                                    ,'06:Educación Media Humanístico Científica Adultos'
                                    ,'08:Educación Media Técnico Profesional y Artística, Adultos')
                AND D.Description = 'K12 School';"""

      _s2 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType C
                ON B.RefOrganizationTypeId = C.RefOrganizationTypeId
                WHERE date(a.ExitDate) = ?
                AND (c.Description = 'Course Section' or c.Description = 'Course');"""

      _s3 = """SELECT digitalRandomKey,
                      fileScanBase64,
                      observaciones
                FROM RoleAttendanceEvent
              WHERE OrganizationPersonRoleId = ?
                AND (RefAttendanceEventTypeId = 2 OR RefAttendanceEventTypeId = 5)
                AND RefAttendanceStatusId = 5
                AND date(Date) = ?
                and time(Date) = ?;"""

      _s4 = """SELECT a.OrganizationPersonRoleId
                      ,date(a.ExitDate) as ExitDate
                      ,time(a.ExitDate) as ExitTime
                      ,a.personId
                      ,a.RoleId
                FROM OrganizationPersonRole a
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN RefOrganizationType C
                ON B.RefOrganizationTypeId = C.RefOrganizationTypeId
                WHERE date(a.ExitDate) = ?
                AND time(a.ExitDate) = ?
                AND c.Description = 'K12 School';"""

      _s5 = """SELECT A.RelatedPersonId ,A.RetirarEstudianteIndicador
                    FROM PersonRelationship A
                    WHERE A.personId = ?"""

      #VERIFICA SI EXISTE REGISTRO DE RETIROS ANTICIPADOS DEL ESTABLECIMIENTO (OrganizationPersonRole)
      logger.info(f"VERIFICA CONSISTENCIA EN FECHA Y HORA DE REGISTROS DE RETIRO Y LA EXISTENCIA DE FIRMA DIGITAL Y/O DOCUMENTO DIGITALIZADO DE AUTORIZACION PARA EL CASO DE QUIEN RETIRA.")
      _r = conn.execute(_s1).fetchall()
      if(len(_r)>0):
        for r in _r:
          _o = r[0]
          _f = r[1]
          _t = r[2]
          _p = r[3]
          #VERIFICA SI EXISTE REGISTRO DE RETIRO DE CLASES PREVIO AL RETIRO DEL ESTABLECIMIENTO EN LA MISMA FECHA (OrganizationPersonRole)
          _v = (str(_f))
          _r2 = conn.execute(_s2, _v).fetchall()
          if (len(_r2)>0):
            for r2 in _r2:
              _o2 = r2[0]
              _f2 = r2[1]
              _t2 = r2[2]
              _p2 = r2[3]

              if(_p == _p2):
                _v2 = (str(_o2),str(_f2),str(_t2))
                _r3 = conn.execute(_s3, _v2).fetchall()
                if(len(_r3)>0):
                  for r3 in _r3:
                    _drk = r3[0]
                    if _drk is None:
                      logger.error(f"Registro de salida de clases no tiene firma de docente (OrganizationPersonRole).")
                      logger.error(f"Rechazado")
                      return False
                else:
                  logger.error(f"No hay registro de retiro de clases (RoleAttendanceEvent)")
                  logger.error(f"Rechazado")
                  return False

              else:
                _v2 = (str(_o2),str(_f2),str(_t2))
                _r3 = conn.execute(_s3, _v2).fetchall()
                if(len(_r3)>0):
                  for r3 in _r3:
                    _drk2 = r3[0]

                    if _drk2 is None:
                      logger.error(f"Registro de salida de clases no tiene firma de docente (RoleAttendanceEvent).")
                      logger.error(f"Rechazado")
                      return False
          else:
            logger.error(f"NO existe registro de salida de clases previo al retiro del establecimiento del alumno.")
            logger.error(f"Rechazado")
            return False

          #VERIFICA SI EXISTE REGISTRO DE RETIRO DE ESTABLECIMIENTO Y QUE COINCIDA CON FECHA Y HORA (RoleAttendanceEvent)
          _v4 = (str(_f),str(_t))
          _r4 = conn.execute(_s4, _v4).fetchall()
          if(len(_r4)>0):
            for r4 in _r4:
              _o4 = r4[0]
              _f4 = r4[1]
              _t4 = r4[2]
              _p4 = r4[3]
              _rl4 = r4[4]

              _v5 = (str(_o4),str(_f4),str(_t4))
              _r5 = conn.execute(_s3, _v5).fetchall()
              #logger.info(f"_o4 {str(_o4)},_f4 {str(_f4)},_t4 {str(_t4)},_rl {str(_rl4)}")
              if(len(_r5)>0):
                for r5 in _r5:
                  _drk = r5[0]
                  _fsb = r5[1]
                  _obs = r5[2]
                  if(_rl4 == 6):
                    if(_drk is None and _obs is None):
                      logger.error(f"Falta firma y observacion en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False
                  elif(_rl4 == 11):
                    if _drk is None:
                      logger.error(f"Falta firma de administrativo en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False
                  else:
                    _v6 = (_p4)
                    _r6 = conn.execute(_s5, _v6).fetchall()
                    if(len(_r6)>0):
                      for r6 in _r6:
                        _pr = r6[1]
                        if not _pr:
                          logger.info(f"La persona que retira a alumno no figura como autorizado en el sistema.")
                          logger.info(f"Apobado")
                          return True
                    if(_drk is None and _fsb is None):
                      logger.error(f"Falta firma o documento digitalizado de apoderado en registro de retiro de estudiante de establecimiento.")
                      logger.error(f"Rechazado")
                      return False

        return True
      else:
        logger.info(f"NO existen registros de retiro anticipado del establecimiento de alumnos.")
        logger.info(f"Apobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de retiros anticipados: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin FN0FB ###

### inicio fn1FA ###
  def fn1FA(self, conn):
    _Apo=[]
    try:

     # VALIDO LA EXISTENCIA DE ALUMNOS RETIRADOS Y QUE TENGAN REGISTRADA FECHA DE RETIRO
      _s1 = """SELECT A.personId,B.Identifier,C.OrganizationPersonRoleId ,C.ExitDate
                FROM PersonStatus A
                JOIN PersonIdentifier B
                ON A.personId = B.personId
                JOIN OrganizationPersonRole C
                ON A.personId = C.personId
                where A.RefPersonStatusTypeId = 30;"""

      # OBTENGO INFORMACION DE APODERADO
      _s2 = """SELECT A.RelatedPersonId ,A.personId
                FROM PersonRelationship A
                JOIN OrganizationPersonRole B
                ON A.RelatedPersonId = B.personId
                WHERE A.personId = ?
                AND B.RoleId = 15;"""

      # OBTENGO ID DE INCIDENTE ASOCIADO
      _s3 = """SELECT A.IncidentId
                FROM IncidentPerson A
                JOIN Incident B
                ON A.IncidentId = B.IncidentId
                WHERE A.personId = ?
                AND B.RefIncidentBehaviorId = 33;"""

      # OBTENGO INFORMACION DE PERSONAS ASOCIADAS A INCIDENTE
      _s4 = """SELECT A.personId,A.RefIncidentPersonTypeId ,A.digitalRandomKey, A.fileScanBase64
                FROM IncidentPerson A
                WHERE A.IncidentId = ?;"""

      #VERIFICA SI EXISTE REGISTRO DE RETIROS ANTICIPADOS DEL ESTABLECIMIENTO (OrganizationPersonRole)
      logger.info(f"VERIFICA LA EXISTENCIA DE REGISTRO DE RETIROS DE ESTUDIANTES DEL ESTABLECIMIENTO.")
      _r = conn.execute(_s1).fetchall()
      if(len(_r)>0):
        _p = self.convertirArray2DToList(list([m[0] for m in _r if m[0] is not None]))
        _i = self.convertirArray2DToList(list([m[1] for m in _r if m[1] is not None]))
        _opr = self.convertirArray2DToList(list([m[2] for m in _r if m[2] is not None]))
        _ed = self.convertirArray2DToList(list([m[3] for m in _r if m[3] is not None])) 
        #VALIDO QUE REIGISTRO DE RETIRO TENGA FECHA DEL EVENTO
        if (len(_p)>len(_ed)):
          logger.error(f"Existen registros de retiros de estudiantes del establecimiento sin fecha de evento.")
          logger.error(f"Rechazado")
          return False
        else:
          for p in _p:
            _v = str(p)
            _r2 = conn.execute(_s2,_v).fetchall()
            if(len(_r2)>0):  
              for rp in _r2:
                _v3 = str(rp[0])
                _r3 = conn.execute(_s3, _v3).fetchall()
                if(len(_r3)>0):
                  for r3 in _r3:
                    _v4 = r3
                    _r4 = conn.execute(_s4, _v4).fetchall()
                    if(len(_r4)>0):
                      for r4 in _r4:
                        va1=str(r4[2])
                        va2=str(r4[3])
                        if(str(r4[1]) == "44"): #docente
                          if va1 is None:
                            logger.error(f"No hay registro de firma digital de docente / administrativo para incidente.")
                            logger.error(f"Rechazado")
                            return False
                        elif(str(r4[1]) == "43"): #apoderado
                          if va1 is None: 
                            if va2 is None:
                              logger.error(f"No hay registro de firma digital ni documento digitalizado de apoderado para incidente.")
                              logger.error(f"Rechazado")
                              return False
                    else:
                      logger.error(f"No hay registro de personas asociadas a incidente Id: {str(r3)}")
                      logger.error(f"Rechazado")
                      return False

                else:
                  logger.error(f"No hay registro de entrega de informacion por retiro de estudiante de establecimiento.")
                  logger.error(f"Rechazado")
                  return False
      else:
        logger.info(f"NO existen registros de retiro de alumnos del establecimiento.")
        logger.info(f"Aprobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn1FA ###

### inicio fn1FB ###
  def fn1FB(self, conn):
    try:
      _l = []
      _l2 = []
      # OBTENGO LISTADO DE ALUMNOS
      _s1 = """SELECT personId,run
                FROM personList
                WHERE Role like '%Estudiante%';"""

      # OBTENGO INFORMACION DE PERSONAS RELACIONADAS CON ALUMNO REGISTRADAS EN EL SISTEMA
      _s2 = """SELECT A.RelatedPersonId,D.RUN
                FROM PersonRelationship A
                JOIN OrganizationPersonRole B
                ON A.RelatedPersonId = B.personId
                JOIN Role C
                ON B.RoleId = C.RoleId
                JOIN personList D
                ON A.RelatedPersonId = D.personId
                WHERE A.personId = ?
                AND B.RoleId = 15;"""

      # OBTENGO ID DE REGISTRO DE ENTREGA DE INFORMACION DE INTERES A LOS APODERADOS
      _s3 = """SELECT A.IncidentId
                FROM IncidentPerson A
                JOIN Incident B
                ON A.IncidentId = B.IncidentId
                WHERE A.personId = ?
                AND B.RefIncidentBehaviorId = 35;"""

      # OBTENGO DETALLE DE EVENTO Y VALIDO FIRMA DE DOCENTE/ADMINISTRATIVO Y DOCUMENTO DIGITALIZADO
      _s4 = """SELECT A.RefIncidentPersonTypeId,A.digitalRandomKey,A.fileScanBase64,C.run
                FROM IncidentPerson A
                JOIN personList C
                ON A.personId = C.personId
                WHERE A.IncidentId = ?;"""

      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          _p = str(q1[0])
          _r = str(q1[0])
          _q2 = conn.execute(_s2,_p).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              _p1 = str(q2[0])
              _r2 = str(q2[1])
              _q3 = conn.execute(_s3,_p1).fetchall()
              logger.info(len(_q3))
              if(len(_q3)!=0):
                for q3 in _q3:
                  _i = str(q3[0])
                  if(_i is None):
                    _l2.append(_r2)
                  else:                  
                    _q4 = conn.execute(_s4,_i).fetchall()
                    if(len(_q4)!=0):
                      _lst = self.convertirArray2DToList(list([m[0] for m in _q4 if m[0] is not None]))
                      if '44' in _lst and '43' in _lst:
                        for q4 in _q4:
                          _pr = q4[0]
                          if(str(_pr)=="44"): #docente
                            _rdk = str(q4[1])
                            if(_rdk is None):
                              logger.error(f"No hay registro de firma de docente para evento de entrega de informacion de interes.")
                              logger.error(f"Rechazado")
                              return False
                          elif(str(_pr)=="43"): #apoderado
                              _fsb = str(q4[2])
                              if(_fsb is None):
                                logger.error(f"No hay registro de documento digitalizado entregado a apoderado para evento de entrega de informacion de interes.")
                                logger.error(f"Rechazado1")
                                return False
                      else:
                        logger.error(f"No hay registro de docente y/o apoderado para evento de entrega de informacion de interes.")
                        logger.error(f"Rechazado")
                        return False

                    else:
                      logger.error(f"No hay registro de personas asociadas al evento de entrega de informacion de interes.")
                      logger.error(f"Rechazado")
                      return False

                if(len(_l2)>0):
                  logger.error(f"Los siguientes apoderados no tienen registro de entrega de informacion de interes por parte del establecimiento: {str(_l2)}")
                  logger.error(f"Rechazado")
                  return False

              else:
                logger.error(f"No hay registro de entrega de informacion de interes del establecimiento al apoderado.")
                logger.error(f"Rechazado")
                return False

          else:
            _l.append(_r)           

        if(len(_l)>0):
          logger.error(f"Los siguientes alumnos no tienen informacion de apoderado asociado en el sistema: {str(_l)}")
          logger.error(f"Rechazado")
          return False
        else:
          logger.info(f"Aprobado")
          return True        

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn1FB ###

### inicio fn1FC ###
  def fn1FC(self, conn):
    try:
      _l = []
      _l2 = []
      # OBTENGO LISTADO DE ALUMNOS
      _s1 = """SELECT A.personId,B.Identifier,C.OrganizationPersonRoleId ,C.ExitDate
                FROM PersonStatus A
                JOIN PersonIdentifier B
                ON A.personId = B.personId
                JOIN OrganizationPersonRole C
                ON A.personId = C.personId
                where A.RefPersonStatusTypeId = 30;"""

      # OBTENGO INFORMACION DE PERSONAS RELACIONADAS CON ALUMNO REGISTRADAS EN EL SISTEMA
      _s2 = """SELECT A.RelatedPersonId,D.RUN
                FROM PersonRelationship A
                JOIN OrganizationPersonRole B
                ON A.RelatedPersonId = B.personId
                JOIN Role C
                ON B.RoleId = C.RoleId
                JOIN personList D
                ON A.RelatedPersonId = D.personId
                WHERE A.personId = ?
                AND B.RoleId = 15;"""

      # OBTENGO ID DE REGISTRO DE ENTREGA DE INFORMACION DE INTERES A LOS APODERADOS
      _s3 = """SELECT A.IncidentId
                FROM IncidentPerson A
                WHERE A.personId = ?
                AND B.RefIncidentBehaviorId = 36;"""

      # OBTENGO DETALLE DE EVENTO Y VALIDO FIRMA DE DOCENTE/ADMINISTRATIVO Y DOCUMENTO DIGITALIZADO
      _s4 = """SELECT A.RefIncidentPersonTypeId,A.digitalRandomKey,A.fileScanBase64,C.run
                FROM IncidentPerson A
                JOIN personList C
                ON A.personId = C.personId
                WHERE A.IncidentId = ?;"""

      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          _p = str(q1[0])
          _r = str(q1[0])

          _q2 = conn.execute(_s2,_p).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              _p1 = str(q2[0])
              _r2 = str(q2[1])

              _q3 = conn.execute(_s3,_p1).fetchall()
              if(len(_q3)!=0):
                for q3 in _q3:
                  _i = str(q3[0])
                  if(_i is None):
                    _l2.append(_r2)
                  else:                  
                    _q4 = conn.execute(_s4,_i).fetchall()
                    if(len(_q4)!=0):
                      _lst = self.convertirArray2DToList(list([m[0] for m in _q4 if m[0] is not None]))
                      if '44' in _lst and '43' in _lst:
                        for q4 in _q4:
                          _pr = str(q4[0])
                          if(str(_pr)=="44"): #docente
                            _rdk = str(q4[1])
                            if(_rdk is None):
                              logger.error(f"No hay registro de firma de docente/administrativo para evento.")
                              logger.error(f"Rechazado")
                              return False
                          elif(str(_pr)=="43"): #apoderado
                              _fsb = str(q4[2])
                              if(_fsb is None):
                                logger.error(f"No hay registro de documento digitalizado entregado a apoderado para evento.")
                                logger.error(f"Rechazado")
                                return False
                      else:
                        logger.error(f"No har registro de docente y/o apoderado para evento.")
                        logger.error(f"Rechazado")
                        return False

                    else:
                      logger.error(f"No hay registro de personas asociadas al evento.")
                      logger.error(f"Rechazado")
                      return False

                if(len(_l2)>0):
                  logger.error(f"Los siguientes apoderados no tienen registro de evento: {str(_l2)}")
                  logger.error(f"Rechazado")
                  return False

              else:
                logger.error(f"No hay registro de entrega de informacion al apoderado.")
                logger.error(f"Rechazado")
                return False

          else:
            _l.append(_r)

        if(len(_l)>0):
          logger.error(f"Los siguientes alumnos no tienen informacion de apoderado asociado en el sistema: {str(_l)}")
          logger.error(f"Rechazado")
          return False

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn1FC ###

### inicio fn6F0  ##
  def fn6F0(self,conn):
    arr=[]
    try:
           # select para listar todos los colegios de tabla organizacion
      _S1= """ SELECT a.OrganizationId,a.Name,b.OrganizationCalendarId,strftime('%Y-%m-%d',c.FirstInstructionDate) as FirstInstructionDate,strftime('%Y-%m-%d',c.LastInstructionDate) AS  LastInstructionDate
                FROM Organization a
                JOIN OrganizationCalendar b 
                ON a.OrganizationId=b.OrganizationId
                JOIN OrganizationCalendarSession c
                ON  b.organizationcalendarid=c.organizationcalendarid where a.Reforganizationtypeid=10;"""

      # trae la fechas para calcular los dias feriados 
      _s2=""" select strftime('%Y-%m-%d',StartDate) as StartDate,strftime('%Y-%m-%d',EndDate) as EndDate from OrganizationCalendarCrisis where OrganizationId = ?;"""

       # select para ver todos los dias de eventos por cada organizacion 
      _s3=""" select * from OrganizationCalendarEvent where OrganizationCalendarId = ?;"""


       # contabilizar los crisis de un colegio 
      _s4=""" select b.RUN,strftime('%Y-%m-%d',a.EntryDate) as EntryDate,strftime('%Y-%m-%d',a.ExitDate) as ExitDate from OrganizationPersonRole a
                join personlist b
                on a.personid=b.personId 
                where OrganizationId = ? and roleId=6 ;"""
    

      now=datetime.now()
      _q1 = conn.execute(_S1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          org_id=str(q1[0])
          org_ca=str(q1[2])
          fecha_in=str(q1[3])
          fecha_ter=str(q1[4])
          f1=datetime.strftime(now, '%Y-%m-%d')

          if (f1 <= fecha_ter):
            fecha_ter=f1

          
          diastotal=int(np.busday_count(fecha_in,fecha_ter))
          
          
          _q2 = conn.execute(_s2,org_id).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
                f2x=str(q2[0])
                f2=str(q2[1])

                if (f1 <= fecha_ter):
                  f2=f1

                
                diastotal2=int(np.busday_count(f2x,f2))
                if diastotal2> diastotal :
                  contador2=diastotal2-diastotal

                else:
                  contador2= diastotal-diastotal2

          elif(len(_q2)==0): 
            contador2= diastotal
          
          _q3 = conn.execute(_s3,org_ca).fetchall()
          if(len(_q3)!=0):
            xx=len(_q3)

            if int(xx)>contador2:

              contador3=int(xx)-contador2

            else:

              contador3=contador2-int(xx)

          elif(len(_q3)==0):
            contador3=contador2
           
          _q4 = conn.execute(_s4,int(org_id)).fetchall()
          if(len(_q4)!=0):
            for w1 in _q4:
              personid=str(w1[0])
              fecha1w=str(w1[1])
              fecha2w=str(w1[2])

              if str(w1[1]) is None:
                fecha1w=fecha_in

              if str(w1[2]) is None:
                fecha2w=fecha_ter
              
              if (f1 <= fecha1w):
                fecha2w=f1

              diastotal3=int(np.busday_count(fecha1w,fecha2w))
              if diastotal3 < (contador2 + contador3):
                diastotal3 = (contador2 + contador3)-diastotal3
              else:
                diastotal3 = diastotal3 - (contador2 + contador3)
              
              if(contador3!=diastotal3):
                arr.append(personid)

            if(len(arr)!=0):
              logger.error(f"Los siguientes alumnos no tienen la cantidad asistencia igual que el establecimiento : {str(arr)} ")
              logger.error(f"Rechazado")
              return False  

          else:  
              logger.error(f"No hubo informacion de resgistros de estudiantes asociados del establecimiento. ")
              logger.error(f"Rechazado")
              return False    

      else:
        logger.error(f"No hay informacion de establecimiento.")
        logger.error(f"Rechazado")
        return False   

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn6F0 ###

### inicio fn6F1 ###
  def fn6F1(self,conn):
    arr=[]
    try:
           
      _S1= """ select strftime('%d',b.Date) as Dia,strftime('%m',b.Date) as Mes,strftime('%Y',b.Date) as Año,a.Identifier as Numerolista ,b.VirtualIndicator 
                        from PersonIdentifier a join RoleAttendanceEvent b on b.OrganizationPersonRoleId=c.OrganizationPersonRoleId 
                         join  OrganizationPersonRole c 
                        where a.RefPersonIdentificationSystemId=54  and c.roleid=6 and b.VirtualIndicator=0 ;"""
    

      now=datetime.now()
      _q1 = conn.execute(_S1).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          dia=str(q1[0])
          mes=str(q1[1])
          año=str(q1[2])
          numero_l=str(q1[3])
          estado_asis=str(q1[4])
          
          if (dia is None) or (mes is None) or (año is None) or (numero_l is None) or (estado_asis is None): 
            arr.append(numero_l)

          if int(estado_asis)==0:
            asistencia="Presencial"

          if(len(arr)!=0):
              logger.error(f"Los siguientes numero de lista necesita informacion: {str(arr)} ")
              logger.error(f"Rechazado")
              return False

          else:
              logger.info(f"Ningunos de los registros le falta un dato.")
              logger.info(f"Apobado")
              return True
          

      else:
        logger.error(f"No hay registro Numero de lista asociados .")
        logger.error(f"Rechazado")
        return False   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn6f1  ###

### inicio fn6E2  ###
  def fn6E2(self, conn):
    try:
      _l1 = []
      logger.info(f"fn6E2 - Valida que no exista asistencia registrada para los dias definidos con suspension de clases.")

      _s1 = """SELECT a.Date,c.RUN
                FROM RoleAttendanceEvent a
                JOIN OrganizationPersonRole b
                ON a.OrganizationPersonRoleId = b.OrganizationPersonRoleId
                JOIN personList c
                ON b.personId = c.personId
                WHERE (a.Date in (SELECT Date FROM OrganizationCalendarEvent)
                    OR(a.Date BETWEEN (SELECT StartDate 
                              FROM OrganizationCalendarCrisis) and  
                              (SELECT EndDate 
                                FROM OrganizationCalendarCrisis)));"""     

      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q in _q1:
          _d = str(q[0])
          _r = str(q[1])
          _l1.append(_d+"-"+_r)
          logger.error(f"Existen registros de asistencia para dias con suspension de clases: {str(_l1)}")
          logger.error(f"Rechazado")
          return False
      else:
        logger.error(f"Aprobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn6E2  ###
 
### inicio fn6D0 ###
  def fn6D0(self, conn):
    logger.info(f"fn6D0 - Valida que las altas y bajas de los alumnos esten registradas en el sistema. El registro debe estar en las tablas PersonStatus y OrganizationPersonRole, y sus fechas deben ser las mismas.")
    try:
      _l = []
      _l2 = []
      _s1 = """SELECT a.OrganizationPersonRoleId,C.RUN,a.EntryDate,a.ExitDate,b.StatusStartDate,b.StatusEndDate
                FROM OrganizationPersonRole A
                JOIN PersonStatus B
                ON A.personId = B.personId
                JOIN personList C
                ON B.personId = C.personId
                WHERE A.RoleId = 6
                AND B.RefPersonStatusTypeId = 30;"""
      
      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          _r = str(q1[1])
          _e1 = str(q1[2])
          _e2 = str(q1[3])
          _sd1 = str(q1[4])
          _sd2 = str(q1[5])

          if(_e1 is None) or (_sd1 is None):
            _l.append(_r)
          elif(_e1 != _sd1):
            _l2.append(_r)
        
        if(len(_l)!=0):
          logger.error(f"Hay alumnos sin rergistro de fecha de alta/baja: {str(_l)}")
          logger.error(f"Rechazado")
          return False

        if(len(_l2)!=0):
          logger.error(f"Hay alumnos con inconsistencia en registros de alta/baja: {str(_l2)}")
          logger.error(f"Rechazado")
          return False
        else:
          logger.error(f"Aprobado")
          return True

      else:
        logger.info(f"No hay registros de alta/baja de alumnos en el establecimiento.")
        logger.info(f"Apobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn6D0 ###

### inicio fn6D1 ###
  def fn6D1(self, conn):
    logger.info(f"fn6D0 - Valida que no exista asistencia anterior al ingreso o posterior al retiro de un alumno en el establecimiento.")
    try:
      _l = []
      _l2 = []
      _s1 = """SELECT A.OrganizationPersonRoleId,C.RUN,A.EntryDate,A.ExitDate
                FROM OrganizationPersonRole A
                JOIN PersonStatus B
                ON A.personId = B.personId
                JOIN personList C
                ON B.personId = C.personId
                WHERE B.RefPersonStatusTypeId = 30;"""

      _s2 = """SELECT Date 
                FROM RoleAttendanceEvent
                WHERE OrganizationPersonRoleId = ?
                AND ((Date <= ?) OR (Date >= ?));"""

      _s3 = """SELECT A.FirstInstructionDate,A.LastInstructionDate
                FROM OrganizationCalendarSession A
                JOIN OrganizationCalendar B
                ON A.OrganizationCalendarId = B.OrganizationCalendarId
                JOIN Organization C
                ON B.OrganizationId = C.OrganizationId
                WHERE C.RefOrganizationTypeId = 10;"""

      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          _o = q1[0]
          _r = str(q1[1])
          _d1 = q1[2]
          _d2 = q1[3]
          if(_d1 is None):
            _q2 = conn.execute(_s3).fetchall()
            if(len(_q2)!=0):
              _d1 = _q2[0]
            else:
              logger.error(f"No hay informacion de calendario academico del establecimiento.")
              logger.error(f"Rechazado")
              return False   
          if(_d2 is None):
            _l.append(_r)
          else:
            _q3 = conn.execute(_s2,_o,_d1,_d2).fetchall()
            if(len(_q3)!=0):
              for q3 in _q3:
                _l2.append(_r+"-"+str(q3[0]))
            else:
              logger.info(f"Apobado")
              return True
        
        if(len(_l)!=0):
          logger.error(f"Hay alumnos retirados sin registro de fecha de retiro: {str(_l)}")
          logger.error(f"Rechazado")
          return False

        if(len(_l2)!=0):
          logger.error(f"Hay alumnos que registran asistencia anterior a la fecha de ingreso o posterior a la fecha de retiro del establecimiento: {str(_l2)}")
          logger.error(f"Rechazado")
          return False 

      else:
        logger.info(f"No hay registros de alta/baja de alumnos en el establecimiento.")
        logger.info(f"Apobado")
        return True

    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin fn6D1 ###

### inicio fn6C0 ###
  def fn6C0(self, conn):
    try:
      logger.info(f"fn6C0 - Valida que los alumnos excedentes SIN derecho a pago solo tengan asistencia a nivel de asignatura y NO de curso.")
      _l1 = []
      _s1 = """SELECT c.RUN, A.OrganizationPersonRoleId
                FROM OrganizationPersonRole A
                JOIN Organization B
                ON A.OrganizationId = B.OrganizationId
                JOIN PersonList c
                ON a.personId = c.personId
                JOIN PersonStatus D
                ON A.personId = D.personId
                WHERE A.RoleId = 6
                AND B.RefOrganizationTypeId = 21
                AND D.RefPersonStatusTypeId = 24;"""

      _s2 = """SELECT Date 
                FROM RoleAttendanceEvent
                WHERE OrganizationPersonRoleId = ?;"""

      _q1 = conn.execute(_s1).fetchall()
      if(len(_q1)!=0):
        for q1 in _q1:
          _r = str(q1[0])
          _op = q1[1]
          _q2 = conn.execute(_s2,_op).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              _d = str(q2[0])
              _l1.append(f"Alumno: {str(_r)} - fecha: {str(_d)}")

        if(len(_l1)!=0):
          logger.error(f"Los siguientes alumnos excedentes sin derecho a subvencion tienen registro de asistencia a nivel de curso: {str(_l1)}")
          logger.error(f"Rechazado")
          return False
        else:
          logger.info(f"Apobado")
          return True

      else:
        logger.info(f"No hay registros de alumnos excedentes sin derecho a subvencion en el establecimiento.")
        logger.info(f"Apobado")
        return True

    except Exception as e:
        logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
        logger.error(f"Rechazado")
        return False
### fin fn6C0 ###

### inicio fn6E0 ###
  def fn6E0(self,conn):
    arr=[]
    diaSemana=[]
    numero=0
    try:

      _S3=""" select organizationid from Organization where reforganizationtypeid=22  ;"""

      _S4=""" select ClassMeetingDays,strftime('%H:%M',ClassBeginningTime) as ClassBeginningTime,strftime('%H:%M',ClassBeginningTime) as ClassBeginningTime,  
            ClassPeriod from coursesectionschedule where organizationid=?;"""

      _S5=""" select a.OrganizationPersonRoleId,a.OrganizationId,a.PersonId,a.roleid,strftime('%Y-%m-%d %H:%M',a.EntryDate) as EntryDate, strftime('%Y-%m-%d %H:%M',a.ExitDate) as ExitDate,a.RecordStartDateTime,a.RecordEndDateTime,b.Identifier
                    from  OrganizationPersonRole a 
                    join PersonIdentifier b on a.personId=b.personId  
                    where roleid=6;"""

      _S6=""" select a.OrganizationPersonRoleId,strftime('%Y-%m-%d',b.Date) as Date,b.fileScanbase64,b.observaciones 
                    from OrganizationPersonRole a join RoleAttendanceEvent b on a.OrganizationPersonRoleId= b.OrganizationPersonRoleId
                    where a.OrganizationPersonRoleId= ? and b.Date= ?;"""

      now=datetime.now()
      _q1 = conn.execute(_S3).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          organizationid=str(q1)
          _q2 = conn.execute(_S4,organizationid).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              diaSemana=str(q2[2]).split(",")
              hora_comi=str(q2[3])
              hora_final=str(q2[4])
              periodo=str(q2[5])
              cantidad_letras=int(len(periodo))-1
              periodo2=(periodo[-2:])
              _q3 = conn.execute(_S5).fetchall()
              if(int(periodo2.strip())==2):
                 for q3 in _q3:
                   id_alu=str(q3[8])
                   orgaId=str(q3[0])
                   hora1=str(q3[4])
                   hora2=str(q3[5])
                   dfs=datetime.strptime(hora1[:10],'%Y-%m-%d')
                   nombresemana2=dfs.isoweekday()
                   for aa in diaSemana:
                      if str(aa.lower())=='lunes':
                        numero=0
                      elif str(aa.lower())=='martes':
                        numero=1
                      elif str(aa.lower())=='miercoles': 
                        numero=2
                      elif str(aa.lower())=='jueves':
                        numero=3
                      elif str(aa.lower())=='viernes':
                        numero=4
                      if int(nombresemana2)==int(numero):
                        if datetime.strptime(hora1[11:len(hora1)], '%H:%M')> datetime.strptime(hora_comi[:5], '%H:%M'):
                          _q4 = conn.execute(_S6,orgaId,dfs).fetchall()
                          if(len(_q4)!=0):
                            for q4 in _q4:
                              justi=str(q4[2])
                              obv=str(q4[3])
                              if justi=="None" or obv=="None":
                                arr.append(id_alu)                          
                          else:
                            arr.append(id_alu)  

        if(len(arr)!=0):
          logger.error(f"Los siguientes alumnos llegaron tarde o : {str(arr)} ")
          logger.error(f"Rechazado")
          return False  
        else: 
          logger.info("Aprobado")
          return True        

      else:
        logger.error(f"No hay registro Numero de lista asociados .")
        logger.error(f"Rechazado")
        return False   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
###  fin fn6E0  ###

### inicio  fn681 ###
  def fn681(self,conn):
    arr=[]
    arr2=[]
    arr3=[]
    dias_laborales=[]
    dias_laborales2=[]
    numero=0
    try:
  
      _S1=""" SELECT OrganizationId
                FROM Organization
                WHERE RefOrganizationTypeId = 47;"""

      _S2=""" SELECT Parent_OrganizationId
                FROM OrganizationRelationship
                WHERE OrganizationId = ?;"""
    
      _S3=""" SELECT OrganizationId
                FROM K12Course
                WHERE OrganizationId = ? and RefWorkbasedLearningOpportunityTypeId=1 ;"""      
                
      _S4=""" select personid 
              from OrganizationPersonRole
              where OrganizationId=? and RoleId = 6 ;"""

      _S5=""" select b.Identifier
              from PersonStatus a 
              join personidentifier b 
              on  a.personid = b.personId  
              where a.RefPersonStatusTypeId=35 and a.personid=? """

      now=datetime.now()
      _q1 = conn.execute(_S1).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          parent=str(q1[0])
          _q2 = conn.execute(_S2,parent).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              parent2=str(q2[0])
              _q3 = conn.execute(_S3,parent2).fetchall()
              if(len(_q3)!=0):
                for q3 in _q3:
                  parent3=str(q3[0])
                  _q4 = conn.execute(_S4,parent3).fetchall()
                  if(len(_q4)!=0):
                    for q4 in _q4:
                      personid=str(q4[0])
                      _q5 = conn.execute(_S5,personid).fetchall()
                      if(len(_q5)==0):
                        rut=str(_q5[0])
                        arr.append(rut)

                    if(len(arr)!=0):
                      logger.error(f"Los siguientes alumnos no tienen identificador de Formacion Dual : {str(arr)} ")
                      logger.error(f"Rechazado")
                      return False
                    else:
                      logger.info(f"Aprobado")
                      return True 
                  
                  else:
                    logger.error(f"No tiene alumnos en la asignatura ")
                    logger.error(f"Rechazado")
                    return False  

              else:
                logger.error(f"La asignatura no esta enlazada para que sea de partica profesional")
                logger.error(f"Rechazado")
                return False      
                    
      else:
        logger.info(f"En el colegio no asignaturas de pratica profesional.")
        logger.info(f"Aprobado")
        return True   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin  fn681 ###

### inicio  fn680 ###
  def fn680(self,conn):
    arr=[]
    arr2=[]
    arr3=[]
    arr4=[]
    dias_laborales=[]
    dias_laborales2=[]
    dias_laborales3=[]
    dias_laborales4=[]
    numero=0
    try:

      _S1="""select a.personId,strftime('%Y-%m-%d', b.entrydate) as entrydate,strftime('%Y-%m-%d',b.ExitDate) as ExitDate 
      from person a join organizationpersonrole b on a.personId=b.personId where b.roleid=6 """

      _S6=""" select strftime('%Y-%m-%d',BeginDate) as BeginDate ,strftime('%Y-%m-%d',EndDate) as EndDate 
              from OrganizationCalendarSession Where organizationcalendarsessionid=1"""

      _S7=""" select c.identifier,a.fileScanBase64 from RoleAttendanceEvent a join organizationpersonrole b on a.OrganizationPersonRoleId=b.OrganizationPersonRoleId 
    join PersonIdentifier c on b.personid=c.personId where strftime('%Y-%m-%d',a.Date)=? and  a.RefAttendanceEventTypeId=1 and b.personId=?;"""

      now=datetime.now()        
      _q1 = conn.execute(_S1).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          personid=str(q1[0])
          fecha_entrada= str(q1[1]) 
          fecha_fin=str(q1[2])
          _q6 = conn.execute(_S6).fetchall()
          if(len(_q6)!=0):
            for q6 in _q6:
              fecha_inicio=str(q6[0])
              fecha_termino=str(q6[1])

              if fecha_fin<fecha_termino:
                fecha_ter_x=fecha_fin
              else:
                fecha_ter_x=fecha_termino

              if (fecha_entrada>fecha_inicio): 
                arr4=self.ListaFechasRango(fecha_inicio,fecha_ter_x,conn)
              else:
                arr4=self.ListaFechasRango(fecha_entrada,fecha_ter_x,conn)
                
              for xx2 in arr4:
                fecha=str(xx2)
                fechaxx1=fecha.replace(',','')
                fechaxx2=fechaxx1.replace('(','')
                fechaxx3=datetime.strptime(fechaxx2[2:12],'%Y-%m-%d')
                _q8 = conn.execute(_S7,fechaxx3,personid).fetchall()
                if(len(_q8)!=0):
                  for dd in _q8:
                    rut=str(dd[0])
                    obser=str(dd[1])
                    if obser=="None":
                      arr.append(rut)
            
        if(arr4!=0):      
          logger.error(f"No tiene asistencia este alumno ")
          logger.error(f"Rechazado")
          return False  
        else:
          logger.info(f"Aprobado")
          return True  
          
      else:
        logger.error(f"En el colegio no asignaturas de pratica profesional    .")
        logger.error(f"Rechazado")
        return False   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False
### fin  fn680 ###

### inicio fn682 ###
  def fn682(self,conn):
    arr=[]
    arr2=[]
    arr3=[]
    arr4=[]
    dias_laborales=[]
    dias_laborales2=[]
    dias_laborales3=[]
    dias_laborales4=[]
    numero=0
    try:
  
      _S1=""" SELECT OrganizationId
                FROM Organization
                WHERE RefOrganizationTypeId = 47;"""

      _S2=""" SELECT Parent_OrganizationId
                FROM OrganizationRelationship
                WHERE OrganizationId = ?;"""
    
      _S3=""" SELECT OrganizationId
                FROM K12Course
                WHERE OrganizationId = ? and RefWorkbasedLearningOpportunityTypeId=1 ;"""      
                
      _S4=""" select personid from OrganizationPersonRole
        where OrganizationId=? and RoleId = 6 ;"""

      _S5=""" select b.personid,strftime('%Y-%m-%d',c.EntryDate) as EntryDate,strftime('%Y-%m-%d',c.ExitDate) as ExitDate
      from PersonStatus a join personidentifier b on  a.personid = b.personId  join organizationpersonrole c on b.personid=c.personId  where a.RefPersonStatusTypeId=35 and a.personid=? ;"""

      _S6=""" select strftime('%Y-%m-%d',BeginDate) as BeginDate ,strftime('%Y-%m-%d',EndDate) as EndDate 
              from OrganizationCalendarSession Where organizationcalendarsessionid=1;"""
 
      _S7=""" select * from RoleAttendanceEvent a join organizationpersonrole b on a.OrganizationPersonRoleId=b.OrganizationPersonRoleId 
            where strftime('%Y-%m-%d',a.Date)=? and  a.RefAttendanceEventTypeId=1 and b.personId=? ;"""
      
      
      now=datetime.now()
      _q1 = conn.execute(_S1).fetchall()
      XX=0
      if(len(_q1)!=0):
        for q1 in _q1:
          parent=str(q1)
          _q2 = conn.execute(_S2,parent).fetchall()
          if(len(_q2)!=0):
            for q2 in _q2:
              parent2=str(q2)
              _q3 = conn.execute(_S3,parent2).fetchall()
              if(len(_q3)!=0):
                for q3 in _q3:
                  parent3=str(q3)
                  _q4 = conn.execute(_S4,parent3).fetchall()
                  if(len(_q4)!=0):
                    for q4 in _q4:
                      peronid=str(q4)
                      _q5 = conn.execute(_S5,peronid).fetchall()
                      if(len(_q5)!=0):
                        for xx in _q5:
                          personid2=str(xx[0])
                          fecha_entrada=str(xx[1])
                          fecha_fin=str(xx[2]) 
                          _q6 = conn.execute(_S6).fetchall()
                          if(len(_q6)!=0):
                            for xx in _q6: 
                              fecha_inicio=str(xx[0])
                              fecha_termino=str(xx[1])
                              if fecha_fin<fecha_termino:
                                fecha_ter_x=fecha_fin
                              else:
                                fecha_ter_x=fecha_termino
                              if (fecha_entrada>fecha_inicio): 
                                arr=self.ListaFechasRango(fecha_inicio,fecha_ter_x,conn)
                              else:
                                arr=self.ListaFechasRango(fecha_entrada,fecha_ter_x,conn)
                 
                          for xx2 in arr:
                            fecha=str(xx2)
                            fechaxx1=fecha.replace(',','')
                            fechaxx2=fechaxx1.replace('(','')
                            fechaxx3=datetime.strptime(fechaxx2[1:11],'%Y-%m-%d')
                            _q8 = conn.execute(_S7,fechaxx3,personid2).fetchall()
                            if(len(_q8)==0):
                              arr4.append(personid2)

                          if(len(arr4)!=0):
                            logger.error(f"Los siguientes alumnos no tienen asistencia:{str(arr4)}")
                            logger.error(f"Rechazado")
                            return False
                          else:
                            logger.info(f"Aprobado")
                            return True
                  else:
                    logger.error(f"No tiene alumnos en la asignatura ")
                    logger.error(f"Rechazado")
                    return False  

              else:
                logger.error(f"La asignatura no esta enlazada para que sea de partica profesional")
                logger.error(f"Rechazado")
                return False  
                    
      else:
        logger.error(f"En el colegio no asignaturas de pratica profesional    .")
        logger.error(f"Rechazado")
        return False   
    
    except Exception as e:
      logger.error(f"NO se pudo ejecutar la consulta de entrega de informaciÓn: {str(e)}")
      logger.error(f"Rechazado")
      return False

### fin  fn682  ###

# Lista de fechas rango de rango fechas menos sabado y domingo
  def ListaFechasRango(self,fecha_ini,fecha_ter,conn):
    arr=[]
    arr2=[]
    arr3=[]
    arr4=[]
    arr5=[]
    arr6=[]
    arr7=[]
    _Trae_fechas= """     WITH RECURSIVE dates(date) AS (
                        VALUES (?)
                        UNION ALL
                        SELECT date(date, '+1 day')
                        FROM dates
                        WHERE date < ?
                        )
                        SELECT strftime('%Y-%m-%d',date) as date  FROM dates;"""

    _S3x=""" select strftime('%Y-%m-%d',StartDate) as StartDate , strftime('%Y-%m-%d',EndDate) as EndDate from OrganizationCalendarCrisis;"""  

    _S3x2="""select strftime('%Y-%m-%d',EventDate) as EventDate from OrganizationCalendarevent where strftime('%Y-%m-%d',EventDate)>= ?;""" 
 
    fecha_in=datetime.strptime(fecha_ini,'%Y-%m-%d')
    fecha_te=datetime.strptime(fecha_ter,'%Y-%m-%d')
    logger.info(fecha_in)
    logger.info(fecha_te)
    _q1 = conn.execute(_Trae_fechas,fecha_in,fecha_te).fetchall()
    if(len(_q1)!=0):
      for q1 in _q1:
        fecha=str(q1)
        fechaxx1=fecha.replace(',','')
        fechaxx2=fechaxx1.replace('(','')
        fechaxx3=datetime.strptime(fechaxx2[1:11],'%Y-%m-%d')
        if int(fechaxx3.isoweekday())!=6 : #sabado
          if int(fechaxx3.isoweekday())!=7: #domingo
            arr.append(str(datetime.strftime(fechaxx3,'%Y-%m-%d')))

      arr3=np.array(arr)  
      arr2.append(np.unique(arr3))

    _q2 = conn.execute(_S3x).fetchall()
    if(len(_q2)!=0):
      for q2 in _q2:
        fecha_com=datetime.strptime(q2[0],'%Y-%m-%d')
        fecha_fin=datetime.strptime(q2[1],'%Y-%m-%d')
        _q1 = conn.execute(_Trae_fechas,fecha_com,fecha_fin).fetchall()
        if(len(_q1)!=0):
          for q1 in _q1:
            fecha=str(q1)
            fechaxx1=fecha.replace(',','')
            fechaxx2=fechaxx1.replace('(','')
            fechaxx3=datetime.strptime(fechaxx2[1:11],'%Y-%m-%d')
            if int(fechaxx3.isoweekday())!=6 : #sabado
              if int(fechaxx3.isoweekday())!=7: #domingo
                arr.append(str(datetime.strftime(fechaxx3,'%Y-%m-%d')))

    for ar in arr2:
      dia=datetime.strptime(str(ar[0]),'%Y-%m-%d')
      for ar2 in arr:
        dia2=datetime.strptime(str(ar2),'%Y-%m-%d')
        if dia!=dia2:
          arr4.append(str(datetime.strftime(dia,'%Y-%m-%d')))

    _q3 = conn.execute(_S3x2,fecha_in).fetchall()
    if(len(_q3)!=0):
      for q3 in _q3:  
        fecha=str(q3)
        fechaxx1=fecha.replace(',','')
        fechaxx2=fechaxx1.replace('(','')
        fechaxx3=datetime.strptime(fechaxx2[1:11],'%Y-%m-%d')
        dia=fechaxx3
        for ar in arr4:
          fecha=str(ar)
          fechaxx1=fecha.replace(',','')
          fechaxx2=fechaxx1.replace('(','')
          fechaxx3=datetime.strptime(fechaxx2[0:11],'%Y-%m-%d')
          if dia!=fechaxx3:
            arr5.append(str(datetime.strftime(dia,'%Y-%m-%d')))  

    arr6=np.array(arr5)  
    arr7.append(np.unique(arr6))
    return arr7

### MIAULA FIN ###