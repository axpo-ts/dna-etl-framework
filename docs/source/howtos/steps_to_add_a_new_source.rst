Steps to add a new source
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a staging config -
   resources/source/{source_name}/templates/src2stg_{source_name}_{name_of_config}_template.yaml
2. Create a bronze config for source data -
   resources/source/{source_name}/templates/stg2brz_{source_name}_{name_of_config}_template.yaml
3. Create a bronze config for attributes table -
   resources/source/{source_name}/templates/stg2brz_{source_name}_{name_of_config}_template.yaml
4. Create a silver config to promote data to silver -
   resources/source/{source_name}/templates/brz2slv_{source_name}_{name_of_config}_template.yaml
5. Create a silver config to generate a view in silver with the
   attributes data and the newly promoted data, along with an
   accompanying SQL file -
   resources/source/{source_name}/templates/brz2slv_{source_name}_{name_of_config}_template.yaml
   and resources/source/{source_name}/sql/{name_of_config}.sql
6. Create a deployment file (job definition), variable file and a readme
   for other developers to extend the source -
   resources/source/{source_name}/{source_name}_deployment.yml,
   resources/source/{source_name}/{source_name}_variables.yaml and
   resources/source/{source_name}/README.md
