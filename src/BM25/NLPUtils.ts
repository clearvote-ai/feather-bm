var nlp_utils = require( 'wink-nlp-utils' );


export function expandQueryToTokens(query: string) : string[]
{
    const lowerCase = nlp_utils.string.lowerCase( query );
    const tokens = nlp_utils.string.tokenize0( lowerCase );
    const removeWords = nlp_utils.tokens.removeWords( tokens );
    const stemmed = nlp_utils.tokens.stem( removeWords );
    const query_expansion : string[] = stemmed;

    return query_expansion;
}