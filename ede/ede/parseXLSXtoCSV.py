# -*- coding: utf-8 -*-
# https://medium.com/swlh/python-flask-with-graphql-server-with-sqlalchemy-and-graphene-and-sqlite-ac9fcc9d3d83
from ede.ede._logger import logger
import pandas as pd
from zipfile import ZipFile
import json
import os
import csv
from anytree import Node, RenderTree, PreOrderIter, findall
from typing import Tuple, Dict, Literal
#import re


class parse:
    def __init__(self, args):
        self.args = args
        logger.info(
            f"typo de argumento: {type(self.args)}, valores: {self.args}")

    def execute(self) -> Literal[True]:
        xd = self.cargarPlanillaConDatosDelModelo()
        df, fileName = self.readExcelData(
            self.args.path_to_file, self.args.nozip)
        node = self.leerTodosLosRegistrosDeLGrupoOrganizaciones(df, xd[(
            xd["JSONGroup"] == '_Organizaciones') | (xd["JSONGroup"] == '_Cursos')].copy())
        node = self.leerTodosLosRegistrosDeLGrupoPersonas(
            df, xd[(xd["JSONGroup"] == '_Personas')].copy(), node)

        _t = f'Archivo {self.args.source} completamente transformado.'
        logger.info(_t)
        return True

    # Carga planilla con todas las tablas y campos del modelo https://ceds.ed.gov
    def cargarPlanillaConDatosDelModelo(self) -> pd.DataFrame:
        #idFile = '1gBhiRXswoCN6Ub2PHtyr5cb6w9VMQgll'
        #url = f'http://drive.google.com/uc?export=download&id={idFile}'
        url = './ede/ede/NDS-Reference-v7_1.xlsx'
        xd = pd.read_excel(url, 'NDS Columns')
        _t = f'Planilla {url} cargada satisfactoriamente'
        logger.info(_t)
        return xd

    def readExcelData(self, path_to_file: str, nozip: bool) -> Tuple[pd.DataFrame, str]:
        # Descomprime el contenido del archivo ZIP y lo carga en memoria
        if(path_to_file):
            if(not nozip):
                with ZipFile(path_to_file, 'r') as zip_ref:
                    zip_ref.extractall('./')
                    _t = f'Archivo ZIP "{path_to_file}" descomprimido con éxito'
                    logger.info(_t)
                    for file in zip_ref.namelist():
                        _t = f"Trabajando sobre archivo: '{file}'"
                        logger.info(_t)
                        df = pd.read_excel(file, sheet_name='_Organizaciones')
                        _t = f"Archivo '{file}' leído sin inconvenientes\n"
                        logger.info(_t)
                        os.remove(file)
            else:
                file = path_to_file
                _t = f"Trabajando sobre archivo: '{file}'"
                logger.info(_t)
                df = pd.read_excel(file, sheet_name='_Organizaciones')
                _t = f"Archivo '{file}' leído sin inconvenientes\n"
                logger.info(_t)

        return df, file

    def getRefOrganizationType(self) -> Dict:
        # NO CAMBIAR el orden del diccionario
        return {
            'K12School': 10,
            'Modalidad': 38,
            'Jornada': 39,
            'Nivel': 40,
            'Rama': 41,
            'Sector': 42,
            'Especialidad': 43,
            'TipoCurso': 44,
            'Enseñanza': 45,
            'Grado': 46,
            'Course': 21,
            'CourseSection': 22,
        }

    def compareList(self, l1: list, l2: list) -> bool:
        l1.sort()
        l2.sort()
        return l1 == l2

    def match_regex(self, node: pd.DataFrame, attr):
        expresion = f"getattr(node, '{attr}', None)"
        attrValue = eval(expresion)
        if attrValue is not None:
            return node

    def leerTodosLosRegistrosDeLGrupoPersonas(self, dfXls: pd.DataFrame, elem: pd.DataFrame, node: pd.DataFrame) -> None:
        # Mapeo de tipos de datos SQL -> Pyhton
        for column in dfXls.columns:
            print("leerTodosLosRegistrosDeLGrupoPersonas->", column)

    def leerTodosLosRegistrosDeLGrupoOrganizaciones(self, dfXls: pd.DataFrame, elem: pd.DataFrame) -> Node:
        # Mapeo de tipos de datos SQL -> Pyhton
        records = []
        elem.sort_values(by=['OrderTable', 'OrderColumn'], inplace=True)
        # En la planilla cada columna tiene el siguiente formato {table}.{column}.{value}
        # Se obtiene una lista de los valores de cada columna.
        organizationTypeList = list(
            set([column.split('.')[2] for column in dfXls.columns]))
        refOrganizationType = self.getRefOrganizationType()

        if not self.compareList(organizationTypeList, list(refOrganizationType.keys())):
            logger.error(
                f"La planilla no contiene todas las columnas requeridas: {organizationTypeList},{list(refOrganizationType.keys())}")

        OrganizationId = 100
        root = Node("root")
        conditions = [{'Query': '', 'Node': root}]
        for organizationType in refOrganizationType.keys():
            #organizationType = 'K12School';
            tmpList = []
            if (f"Organization.Name.{organizationType}" in dfXls.columns):
                for condition in conditions:
                    beforeQuery = condition.get('Query')
                    # logger.info(f"OrganizationType:{organizationType},Condition:{beforeQuery}")
                    df = dfXls.query(beforeQuery) if (
                        beforeQuery and beforeQuery != '') else dfXls
                    for e in df[f"Organization.Name.{organizationType}"].unique():
                        OrganizationId += 1
                        Parent_OrganizationId = condition.get(
                            'OrganizationId', None)
                        OrganizationRelationshipId = OrganizationId + 10000
                        RefOrganizationRelationshipId = 3
                        ShortName = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"Organization.ShortName.{organizationType}"].values[0] if (
                            f"Organization.ShortName.{organizationType}" in dfXls.columns) else None
                        RegionGeoJSON = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"Organization.RegionGeoJSON.{organizationType}"].values[0] if (
                            f"Organization.RegionGeoJSON.{organizationType}" in dfXls.columns) else None
                        RefOrganizationTypeId = refOrganizationType[organizationType]
                        newQuery = beforeQuery + f' & `Organization.Name.{organizationType}` == "{e}" ' if(
                            beforeQuery != '') else f' `Organization.Name.{organizationType}` == "{e}" '

                        OrganizationEmailId = OrganizationId + 10000
                        ElectronicMailAddress = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationEmail.ElectronicMailAddress.{organizationType}"].values[0] if (
                            f"OrganizationEmail.ElectronicMailAddress.{organizationType}" in dfXls.columns) else None
                        RefEmailTypeId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationEmail.RefEmailTypeId.{organizationType}"].values[0] if (
                            f"OrganizationEmail.RefEmailTypeId.{organizationType}" in dfXls.columns) else 3  # The type of electronic mail (e-mail) address listed for a person or organization.
                        Identifier = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationIdentifier.Identifier.{organizationType}"].values[0] if (
                            f"OrganizationIdentifier.Identifier.{organizationType}" in dfXls.columns) else None
                        OrganizationIdentifierId = OrganizationId + 10000
                        RefOrganizationIdentificationSystemId = 48
                        RefOrganizationIdentifierTypeId = 17
                        OrganizationTelephoneId = OrganizationId + 10000
                        TelephoneNumber = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationTelephone.TelephoneNumber.{organizationType}"].values[0] if (
                            f"OrganizationTelephone.TelephoneNumber.{organizationType}" in dfXls.columns) else None
                        PrimaryTelephoneNumberIndicator = 1
                        RefInstitutionTelephoneTypeId = 2
                        Website = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationWebsite.Website.{organizationType}"].values[0] if (
                            f"OrganizationWebsite.Website.{organizationType}" in dfXls.columns) else None
                        OrganizationOperationalStatusId = OrganizationId + 10000
                        RefOperationalStatusId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationOperationalStatus.RefOperationalStatusId.{organizationType}"].values[0] if (
                            f"OrganizationOperationalStatus.RefOperationalStatusId.{organizationType}" in dfXls.columns) else None
                        OperationalStatusEffectiveDate = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"OrganizationOperationalStatus.OperationalStatusEffectiveDate.{organizationType}"].values[0] if (
                            f"OrganizationOperationalStatus.OperationalStatusEffectiveDate.{organizationType}" in dfXls.columns) else None

                        LocationId = OrganizationId + 10000
                        StreetNumberAndName = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.StreetNumberAndName.{organizationType}"].values[0] if (
                            f"LocationAddress.StreetNumberAndName.{organizationType}" in dfXls.columns) else None
                        ApartmentRoomOrSuiteNumber = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.ApartmentRoomOrSuiteNumber.{organizationType}"].values[0] if (
                            f"LocationAddress.ApartmentRoomOrSuiteNumber.{organizationType}" in dfXls.columns) else None
                        BuildingSiteNumber = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.BuildingSiteNumber.{organizationType}"].values[0] if (
                            f"LocationAddress.BuildingSiteNumber.{organizationType}" in dfXls.columns) else None
                        City = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.City.{organizationType}"].values[0] if (
                            f"LocationAddress.City.{organizationType}" in dfXls.columns) else None
                        RefStateId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.RefStateId.{organizationType}"].values[0] if (
                            f"LocationAddress.RefStateId.{organizationType}" in dfXls.columns) else None
                        PostalCode = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.PostalCode.{organizationType}"].values[0] if (
                            f"LocationAddress.PostalCode.{organizationType}" in dfXls.columns) else None
                        CountyName = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.CountyName.{organizationType}"].values[0] if (
                            f"LocationAddress.CountyName.{organizationType}" in dfXls.columns) else None
                        RefCountyId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.RefCountyId.{organizationType}"].values[0] if (
                            f"LocationAddress.RefCountyId.{organizationType}" in dfXls.columns) else None
                        RefCountryId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.RefCountryId.{organizationType}"].values[0] if (
                            f"LocationAddress.RefCountryId.{organizationType}" in dfXls.columns) else None
                        Latitude = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.Latitude.{organizationType}"].values[0] if (
                            f"LocationAddress.Latitude.{organizationType}" in dfXls.columns) else None
                        Longitude = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.Longitude.{organizationType}"].values[0] if (
                            f"LocationAddress.Longitude.{organizationType}" in dfXls.columns) else None
                        RefERSRuralUrbanContinuumCodeId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.RefERSRuralUrbanContinuumCodeId.{organizationType}"].values[0] if (
                            f"LocationAddress.RefERSRuralUrbanContinuumCodeId.{organizationType}" in dfXls.columns) else None
                        FacilityBlockNumberArea = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.FacilityBlockNumberArea.{organizationType}"].values[0] if (
                            f"LocationAddress.FacilityBlockNumberArea.{organizationType}" in dfXls.columns) else None
                        FacilityCensusTract = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"LocationAddress.FacilityCensusTract.{organizationType}"].values[0] if (
                            f"LocationAddress.FacilityCensusTract.{organizationType}" in dfXls.columns) else None

                        OrganizationLocationId = OrganizationId + 10000
                        RefOrganizationLocationTypeId = 2

                        ClassroomIdentifier = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"Classroom.ClassroomIdentifier.{organizationType}"].values[0] if (
                            f"Classroom.ClassroomIdentifier.{organizationType}" in dfXls.columns) else None

                        CourseSectionLocationId = OrganizationId + 10000
                        RefInstructionLocationTypeId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSectionLocation.RefInstructionLocationTypeId.{organizationType}"].values[0] if (
                            f"CourseSectionLocation.RefInstructionLocationTypeId.{organizationType}" in dfXls.columns) else None

                        RefCourseSectionDeliveryModeId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSection.RefCourseSectionDeliveryModeId.{organizationType}"].values[0] if (
                            f"CourseSection.RefCourseSectionDeliveryModeId.{organizationType}" in dfXls.columns) else None
                        RefSingleSexClassStatusId = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSection.RefSingleSexClassStatusId.{organizationType}"].values[0] if (
                            f"CourseSection.RefSingleSexClassStatusId.{organizationType}" in dfXls.columns) else None
                        TimeRequiredForCompletion = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSection.TimeRequiredForCompletion.{organizationType}"].values[0] if (
                            f"CourseSection.TimeRequiredForCompletion.{organizationType}" in dfXls.columns) else None
                        CourseId = Parent_OrganizationId
                        VirtualIndicator = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSection.VirtualIndicator.{organizationType}"].values[0] if (
                            f"CourseSection.VirtualIndicator.{organizationType}" in dfXls.columns) else None
                        MaximumCapacity = dfXls[dfXls[f"Organization.Name.{organizationType}"] == e][f"CourseSection.MaximumCapacity.{organizationType}"].values[0] if (
                            f"CourseSection.MaximumCapacity.{organizationType}" in dfXls.columns) else None

                        node = Node(e,
                                    parent=condition.get('Node') if (
                                        condition.get('Node')) else root,
                                    OrganizationId=OrganizationId,
                                    Name=e,
                                    RefOrganizationTypeId=RefOrganizationTypeId,
                                    Parent_OrganizationId=Parent_OrganizationId,
                                    ShortName=ShortName,
                                    RegionGeoJSON=RegionGeoJSON,
                                    Website=Website,
                                    OrganizationRelationshipId=OrganizationRelationshipId,
                                    RefOrganizationRelationshipId=RefOrganizationRelationshipId,
                                    OrganizationEmailId=OrganizationEmailId,
                                    ElectronicMailAddress=ElectronicMailAddress,
                                    RefEmailTypeId=RefEmailTypeId,
                                    Identifier=Identifier,
                                    OrganizationIdentifierId=OrganizationIdentifierId,
                                    RefOrganizationIdentificationSystemId=RefOrganizationIdentificationSystemId,
                                    RefOrganizationIdentifierTypeId=RefOrganizationIdentifierTypeId,
                                    OrganizationTelephoneId=OrganizationTelephoneId,
                                    TelephoneNumber=TelephoneNumber,
                                    PrimaryTelephoneNumberIndicator=PrimaryTelephoneNumberIndicator,
                                    RefInstitutionTelephoneTypeId=RefInstitutionTelephoneTypeId,
                                    OrganizationOperationalStatusId=OrganizationOperationalStatusId,
                                    RefOperationalStatusId=RefOperationalStatusId,
                                    OperationalStatusEffectiveDate=OperationalStatusEffectiveDate,
                                    LocationId=LocationId,
                                    StreetNumberAndName=StreetNumberAndName,
                                    ApartmentRoomOrSuiteNumber=ApartmentRoomOrSuiteNumber,
                                    BuildingSiteNumber=BuildingSiteNumber,
                                    City=City,
                                    RefStateId=RefStateId,
                                    PostalCode=PostalCode,
                                    RefCountyId=RefCountyId,
                                    RefCountryId=RefCountryId,
                                    Latitude=Latitude,
                                    Longitude=Longitude,
                                    OrganizationLocationId=OrganizationLocationId,
                                    RefOrganizationLocationTypeId=RefOrganizationLocationTypeId,
                                    ClassroomIdentifier=ClassroomIdentifier,
                                    CourseSectionLocationId=CourseSectionLocationId,
                                    RefInstructionLocationTypeId=RefInstructionLocationTypeId,
                                    RefCourseSectionDeliveryModeId=RefCourseSectionDeliveryModeId,
                                    RefSingleSexClassStatusId=RefSingleSexClassStatusId,
                                    TimeRequiredForCompletion=TimeRequiredForCompletion,
                                    CourseId=CourseId,
                                    VirtualIndicator=VirtualIndicator,
                                    MaximumCapacity=MaximumCapacity,
                                    CountyName=CountyName,
                                    RefERSRuralUrbanContinuumCodeId=RefERSRuralUrbanContinuumCodeId,
                                    FacilityBlockNumberArea=FacilityBlockNumberArea,
                                    FacilityCensusTract=FacilityCensusTract,
                                    query=newQuery
                                    )

                        tmpList.append(
                            {'Query': newQuery, 'OrganizationId': OrganizationId, 'Node': node})
                conditions = tmpList
            else:
                logger.error(
                    f"La planilla no contiene todas las columnas requeridas. Organization.Name.{organizationType} no encontrada.")

        #pd.set_option('display.max_rows', None)
        #pd.set_option('display.max_columns', None)
        #pd.set_option('display.width', None)
        # print(dfCSV)
        # print(RenderTree(root))

        self.crearCSV(elem, root)
        return root

    def crearCSV(self, elem: pd.DataFrame, root: Node) -> Literal[True]:
        # Se crea un DF por tabla.
        # Primero se rescata los datos y condiciones de cada tabla desde la planilla publicada.
        # Segundo, se realiza una busqueda de todos los nodos del árbol que cumplen con la condición.
        # Tercero, se rescata la información de cada uno de los nodos.
        # Finalmente, se graba la información en un archivo csv.
        for table in elem['Table'].unique():  # Avanza tabla por tabla
            columnList = list(elem[elem['Table'] == table]['Column'])
            # Se crea un DF por cada Tabla con el nombre de las columnas
            dfCSV = pd.DataFrame(columns=columnList)
            condition = elem[(elem.Table == table) & (
                elem.Condition.notnull())]['Condition'].values
            data = elem[(elem.Table == table) & (
                elem.Data.notnull())]['Data'].values

            if (condition and data):
                nodes = findall(
                    root, filter_=lambda node: self.match_regex(node, condition[0]))
                print(table, len(nodes))
                for node in nodes:
                    dfCSV = dfCSV.append(eval(data[0]), ignore_index=True)

            dfCSV.to_csv(f"./{self.args.path_to_dir_csv_file}/{table}.csv")

        return True
