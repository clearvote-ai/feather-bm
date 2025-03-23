var nlp_utils = require( 'wink-nlp-utils' );


export function expandQueryToTokens(query: string) : string[]
{
    const lowerCase = nlp_utils.string.lowerCase( query );
    const tokens = nlp_utils.string.tokenize0( lowerCase );
    const removeWords = nlp_utils.tokens.removeWords( tokens );
    const stemmed = nlp_utils.tokens.stem( removeWords );
    //who cares what happened before 1776 amirite?
    const number_removal = removeNumbersOutsideRange(stemmed, { min: 1776, max: 2100 });
    const query_expansion : string[] = number_removal;

    return query_expansion;
}

function removeNumbersOutsideRange(tokens: string[], range: { min: number, max: number }) : string[]
{
    return tokens.filter(token => isNumberInsideRange(token, range));
}

function isNumberInsideRange(query: string, range: { min: number, max: number }) : boolean 
{
    //check if the token is a number and if it is outside the range for eg. "1600" is outside 1900-2020
    const number = parseInt(query); 
    if(isNaN(number)) return true; //not a number 
    return number >= range.min && number <= range.max; 
}