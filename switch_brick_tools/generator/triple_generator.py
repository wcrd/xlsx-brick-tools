from argparse import ArgumentError
import rdflib
from rdflib.collection import Collection
import logging
from ..common import helpers

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Create rdf triples from input dataframes
def process_df(df, namespaces:dict, multiIndexHeader:str, relationships_to_process:list, relationship_field:tuple, df_map):
    '''
    :relationship_field: column that is referenced by the relationships fields. Typically 'identifier', but 'label' is also widely used.
    '''
    print(f"Processing {multiIndexHeader} relationships.")
    triples = []

    # validate that multi-index header has been provided
    # validate if input file has SwitchTags
    headerExists = multiIndexHeader in df.columns

    if not headerExists:
        print(f"No {multiIndexHeader} columns have been provided")
        return triples
    
    # validate input df has a valid identifier column (this is used for entity definition).
    # Df must have this column
    # PRIOR METHOD
    # if helpers.validate_relationships(df['Brick'].columns, [("identifier", "Literal", "")]) == []:
    #     print("No valid identifier column found. Aborting.")
    #     return triples
    # NEW METHOD
    if not helpers.column_exists(df.columns, relationship_field):
        logger.error("No valid identifier column found. Aborting.")
        return triples

    # validate input df has all relationships
    relationships = helpers.validate_relationships(df[multiIndexHeader].columns, relationships_to_process)

    # process data for each row
    for idx, row in df.iterrows():

        # set identifier name & class
        identifier = helpers.format_fragment(row['Brick']['identifier'])
        entity_class = helpers.format_fragment(row['Brick']['class'])

        # define entity
        try:
            if entity_class == 0:
                continue
            elif "switch:" in entity_class:
                triples.append((namespaces['building'][identifier], rdflib.RDF.type, namespaces['switch'][entity_class.replace("switch:", "")]))
            else:
                triples.append((namespaces['building'][identifier], rdflib.RDF.type, namespaces['brick'][entity_class]))
        except:
            print(identifier, entity_class, namespaces)
            raise Exception("DEBUG ->> Error in making triples.")

        # create relationships
        for relationship in relationships:
            # prepare data
        
            data = row[multiIndexHeader][relationship.name]
            if data == 0 or data == "" or not data: continue
            data = [x.strip() for x in data.split("|")]

            # convert to ids using the df_map; remove unknown items. Only for references.
            if relationship.datatype == "ref":
                data = [ helpers.lookupValue(df_map, item) for item in data ]
                data = list(filter(None, data))


            if relationship.datatype == "Literal":
                for item in data:
                    triples.append( (namespaces['building'][identifier], namespaces[relationship.namespace][relationship.name], rdflib.Literal( item )) )
            elif relationship.datatype == "brick":
                for item in data:
                    if "switch:" in item:
                        # target is from switch namespace
                        triples.append((namespaces['building'][identifier], namespaces[relationship.namespace][relationship.name], namespaces["switch"][helpers.format_fragment(item.replace("switch:", ""))]))
                    else:
                        # continue as normal
                        triples.append((namespaces['building'][identifier], namespaces[relationship.namespace][relationship.name], namespaces[relationship.datatype][helpers.format_fragment(item)]))
            else:
                for item in data:
                    triples.append((namespaces['building'][identifier], namespaces[relationship.namespace][relationship.name], namespaces[relationship.datatype][helpers.format_fragment(item)]))

        # create switch:tags if they exist

    return triples


# this method is separate for debugging purposes for now
def process_tags(g: rdflib.Graph, df, namespaces: dict, multiIndexHeader: str = "SwitchTags"):
    # validate if input file has SwitchTags
    switchTagsExist = "SwitchTags" in df.columns

    if not switchTagsExist:
        print("No Switch Tags have been provided")
        return

    # iterate rows and create tags on entities
    for idx, row in df.iterrows():
        # validate that row is valid
        identifier = helpers.format_fragment(row['Brick']['identifier'])
        entity_class = helpers.format_fragment(row['Brick']['class'])
        if entity_class == 0: continue

        # get tags
        switchTags = row[multiIndexHeader]

        # generate holding container and list of entities (nodes)
        tag_list = rdflib.BNode()
        nodes = []

        for tagGroup, tagValue in switchTags.items():
            # validate input exists
            if tagGroup == 0 or tagGroup == "" or not tagGroup: continue
            if tagValue == 0 or tagValue == "" or not tagValue: continue

            # split tagValue if multiple are provided
            tagValues = [x.strip() for x in tagValue.split("|")]

            # generate individual k-v pairs
            for tag in tagValues:
                tagDef = rdflib.BNode()  # generate BNode per tag pair
                nodes.append(tagDef)  # record this node in a list that will be used to generate collection
                g.add((tagDef, namespaces["meta"]["key"], rdflib.Literal(tagGroup)))
                g.add((tagDef, namespaces["meta"]["value"], rdflib.Literal(tag)))

        # add to RDF collection and insert into model
        Collection(g, tag_list, nodes)
        g.add((namespaces['building'][identifier], namespaces['switch']['hasTagCollection'], tag_list))


# process source
def process_source(g: rdflib.Graph, df, namespaces:dict, multiIndexHeader: str = "Source" ):
    # validate if input file has source columns
    sourceExists = multiIndexHeader in df.columns
    if not sourceExists:
        print("No source columns have been provided")
        return
    
    # validate core columns are provided
    core_columns = [ 'Device No', 'BACnet Device Name', "IP Address", 'Object Address', 'Object Name', 'BACnet Unit Of Measure' ]
    for field in core_columns:
        if field not in df[multiIndexHeader].columns:
            logger.error(f"Input df does not have all core BACnet fields defined.\nExpected fields: ({', '.join(core_columns)})\nError on: {field}")
            logger.info("Aborting adding BACnet source information to model.")
            return
    
    logger.info("This method only supports BACnet source information for now.")

    # define BACnet namespace
    BACNET = rdflib.Namespace("http://data.ashrae.org/bacnet/2020#")
    g.bind("bacnet", BACNET)
    # define reference namespace
    REF = rdflib.Namespace("https://brickschema.org/schema/Brick/ref#")
    g.bind("ref", REF)

    # iterate rows and create BACnet devices and point external references
    for idx, row in df.iterrows():
        # validate that row is valid
        identifier = helpers.format_fragment(row['Brick']['identifier'])
        entity_class = helpers.format_fragment(row['Brick']['class'])
        if entity_class == 0: continue

        # get source info
        sourceInfo = row[multiIndexHeader]
        
        # process if BACnet
        if str(sourceInfo['Type']).lower() == "bacnet":

            ## generate device object
            #
            # id = network _ device_no
            nw = sourceInfo['BACnet Network']
            id = namespaces['building'][f'dev_{( nw if nw != "" else "0" )}_{sourceInfo["Device No"]}']

            g.add((id, rdflib.RDF.type, BACNET['BACnetDevice']))
            g.add((id, BACNET['device-instance'], rdflib.Literal(sourceInfo["Device No"])))
            g.add((id, BACNET['device-name'], rdflib.Literal(sourceInfo["BACnet Device Name"])))
            g.add((id, BACNET['ip-address'], rdflib.Literal(sourceInfo["IP Address"])))


            ## general external reference bNode on point
            #
            # ref:hasExternalReference [
            #     a ref:BACnetReference ;
            #     bacnet:object-identifier "analog-value,5" ;
            #     bacnet:object-name "BLDG-Z410-ZATS" ;
            #     bacnet:objectOf bldg:sample-device ;
            # ] .
            extRef = rdflib.BNode()
            g.add( (namespaces['building'][identifier], REF['hasExternalReference'], extRef) )
            g.add( (extRef, rdflib.RDF.type, REF['BACnetReference']) )
            g.add( (extRef, BACNET['object-identifier'], rdflib.Literal(sourceInfo['Object Address'])) )
            g.add( (extRef, BACNET['object-name'], rdflib.Literal(sourceInfo['Object Name'])) )
            g.add( (extRef, BACNET['units-of-measure'], rdflib.Literal(sourceInfo['BACnet Unit Of Measure'])) )
            g.add( (extRef, BACNET['object-of'], id) )

