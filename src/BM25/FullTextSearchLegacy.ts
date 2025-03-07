"use server";

import { PromisePool } from "@supercharge/promise-pool";
import { expandQueryToTokens } from "./WinkUtilsWrapper";
import { DocumentIndex, IndexedDocument } from "./InvertedIndex";
import { getDocumentBM25Scores } from "./Search";

var nlp_utils = require( 'wink-nlp-utils' );

// <------BUILD THE INVERTED INDEX------------>


export interface FullTextInlineSearchResult {
    line_number: number;
    line_text: string;
    char_indices: (number | undefined)[]; //start of each occurence of the query in the line
}

// <----------SEARCH THE INVERTED INDEX------------>
export interface ClearTextEngineResult {
    document: IndexedDocument;
    score: number;
    full_text_results?: FullTextInlineSearchResult[];
}

export interface ClearTextEngineResultArray {
    items: ClearTextEngineResult[];
    time_taken: number;
    index_gets: number;
    s3_gets: number;
    full_result_count: number;
}

export interface FullTextInlineSearchResult {
    line_number: number;
    line_text: string;
    char_indices: (number | undefined)[]; //start of each occurence of the query in the line
}

const PAGE_SIZE = 10;
export async function fullTextSearchInternal(
    document_index : DocumentIndex, 
    query: string,
    page: number,
    doc_getter : (document_id: string) => Promise<IndexedDocument>,
    full_text_getter : (document_id: string, version: number) => Promise<string>
) : Promise<ClearTextEngineResultArray>
{
    const start_time = performance.now();
    const { index_gets, scores} = (await getDocumentBM25Scores(query, document_index));

    const top_10_docs = scores
    //paginate the results
    .slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
    //filter out any documents that have no score
    .filter((doc) => doc.score !== undefined);

    //compute the full text search results for each of the top 10 documents
    const { results : full_text_results, errors } = await PromisePool
    .withConcurrency(10)
    .for(top_10_docs)
    //export type ErrorHandler<T> = (error: Error, item: T, pool: Stoppable & UsesConcurrency) => Promise<void> | void;
    .handleError(async (error, token, pool) => {
        console.error("Error processing token index lookup", error);
    })
    .process(async (doc, index, pool) => {
        //TODO: convert this to a dynamoDB Get
        const document_sortkey = document_index.IDToUUIDMap[doc.doc_id];

        const document_id = document_sortkey.split('#')[1];

        const [full_text, metadata] = await Promise.all([
            full_text_getter(document_id, 1),
            doc_getter(document_id)
        ]);

        const full_document = metadata as IndexedDocument;
        //full_document_list.find( (d) => d.sortkey === document_sortkey) as GovernmentDocumentClientSide;
        full_document.full_text = full_text ?? "";
        return { 
            document: full_document as IndexedDocument,
            score: doc.score, 
            full_text_results: await inlineSearch(query, full_text ?? "") 
        };
    });

    const end_time = performance.now();

    return { 
        items: full_text_results.sort((a, b) => b.score - a.score),
        time_taken: end_time - start_time,
        index_gets: index_gets,
        s3_gets: full_text_results.length,
        full_result_count: scores.length
    };
}

async function inlineSearch(query: string, full_text: string) : Promise<FullTextInlineSearchResult[]>
{
    const lines = full_text.split('\n');
    const query_results = new Array<FullTextInlineSearchResult>();

    const query_expansion : string[] = await expandQueryToTokens(query);

    lines.forEach( (line, index) => {
        query_expansion.forEach( (query_expanse) => {
            const matches = line.matchAll( new RegExp(query_expanse, 'gi') );
            const results = Array.from(matches, (match) => {
                const next_space = line.indexOf(' ', match.index ?? 0);
                let end_index = next_space === -1 ? line.length : next_space;
                return { char_indices: [match.index, end_index], line_number: index, line_text: line };
            });

            query_results.push( ...results );
        });
    });

   return query_results;
}