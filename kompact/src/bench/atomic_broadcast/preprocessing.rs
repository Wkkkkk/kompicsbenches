use regex::Regex;
use lazy_static::lazy_static;

// Split a raw sql query into a template and parameters
// A query template contains only the operations but no values
// All parameters are connected as a string by comma
pub fn split_query(query: &str) -> (String, String) {
    lazy_static! {
        static ref RULES: Vec<&'static str> = vec![
            r#"('\d+\\.*?')"#,                // hash values
            r#"'((')|(.*?([^\\])'))"#,        // string
            r#""((")|(.*?([^\\])"))"#,        // double-quoted string
            r#"([^a-zA-Z'(,\*])\d+(\.\d+)?"#,   // integers(prevent us from capturing table name like "a1")
        ];
        static ref REGEX_SETS: Vec<Regex> = RULES.iter()
                    .map(|s| Regex::new(s).unwrap())
                    .collect();
    }

    let mut bitmap = vec![0;query.len()];
    let mut indice_pairs = Vec::new();
    for re in REGEX_SETS.iter() {
        for (index, mat) in query.match_indices(re) {
            if bitmap[index] == 1 {
                continue
            } else {
                for i in index..index+mat.len() {
                    bitmap[i] = 1;
                }
            }
    
            indice_pairs.push((index, mat));
        } 
    }
    let mut template = query.to_string();
    for re in REGEX_SETS.iter() {
        template = re.replace_all(&template, "@").to_string();
    }

    indice_pairs.sort_by_key(|p| p.0);
    // println!("indice_pairs: {:?}", indice_pairs);

    // println!("template: {:?}", template);
    let parameters = indice_pairs.iter()
                        .map(|p| p.1)
                        .collect::<Vec<_>>()
                        .join(",");
    // println!("parameters: {:?}", parameters);
    
    (template, parameters)
}

// Merge template string with parameters
// There should be the exact number of parameters to fill in
pub fn merge_query(template: String, parameters: String) -> String {
    if parameters.is_empty() { return template; }

    let parameter_list = parameters
        .split(",")
        .collect::<Vec<_>>();
    let num_parameters = parameter_list.len();

    let parts = template.split("@").collect::<Vec<_>>();
    if  parts.len() != num_parameters+1 {
        println!("Unmatched templates {} \n and parameters {}", template, parameters);
    }

    let mut query = String::new();
    for i in 0..num_parameters {
        query.push_str(parts[i]);
        query.push_str(parameter_list[i]);
    }
    query.push_str(parts[num_parameters]);

    query
}