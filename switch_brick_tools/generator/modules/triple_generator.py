from argparse import ArgumentError
import rdflib
from rdflib.collection import Collection
import logging
from . import helpers

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


