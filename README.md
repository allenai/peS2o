<p align="center" style="margin-top: -2em">
<img src="https://huggingface.co/datasets/allenai/pes2o/resolve/main/logo.png" alt="peS2o logo. It's a picure of a mortar and pestle with documents flying in." width=384px height=auto>  
</p>
<p align="center" style="font-size: 1.2em; margin-top: -1em"><i>Pretraining Efficiently on <a href="https://github.com/allenai/s2orc">S2ORC</a>!</i></p>
<p align="center" style="font-size: 1.2em;">Available on the <a href="https://github.com/allenai/s2orc">Huggingface Hub</a></p>

The peS2o dataset is a collection of ~40M creative commmon licensed academic papers,
cleaned, filtered, and formatted for pre-training of language models. It is derived from
the [Semantic Scholar Open Research Corpus][2]([Lo et al, 2020][1]), or S2ORC.


We release multiple version of peS2o, each with different processing and knowledge cutoff
date. We recommend you to use the latest version available.

If you use this dataset, please cite:

```bibtex
@techreport{pes2o,
    author = {Luca Soldaini and Kyle Lo},
    year = 2023,
    title = {{peS2o (Pretraining Efficiently on S2ORC) Dataset}},
    institution = {{Allen Institute for AI}},
    note = {\url{https://huggingface.co/datasets/allenai/pes2o}}
}
```

## Document Format

Each document in the dataset is a dictionary with the following fields:

- `added`: Date the document was added to the corpus.
- `created`: Best-guess date for when the document was first published. Some have resolution down to the day, only down to the year.
- `id`: Semantic Scholar Corpus ID of the document; it can be used with the [Semantic Scholar API](https://api.semanticscholar.org/) to retrieve metadata about the document (e.g., fields of study, authors).
- `source`: Collection from which the document was sourced from. At the moment, two are supported:
  - `s2orc`: collection of full-text papers
  - `s2ag`: collection of title and abstracts
- `text`: Text of the document. Paragraphs are separated by two newlines (`\n\n`).
- `version`: version of peS2o.

------

## peS2o V1

### Key Facts

- *Knowledge cutoff*: 2023-01-03
- *Number of documents*: 67.56M
- *Number of whitespace-separated tokens*: 47.37M

### Processing

Processing differs slightly wether it was derived from the full-text corpus (`s2orc`) or the title and abstract corpus (`s2ag`).

#### S2ORC-derived documents

Unfiltered, S2ORC contains 11.3M papers and 46.9B whitespace-separated tokens as of 2023-01-03. To derive peS2o v1, we impose the following constraints:

- The paper must have a title and abstract.
- From each paper, we use [Grobid](https://github.com/kermitt2/grobid) to extract section headers and paragraphs; figures, tables, and references, and any other non-textual content is removed. Title and abstracts are also available, but they come from the Semantic Scholar metadata (obtained through the APIs), not Grobid.
- The paper must be in English.
  - To determine the language of each document, we use the [pycld3](https://github.com/bsolomon1124/pycld3) library
  - We run pycld3 on the first 2000 characters of each paragraph in the paper.
  - The language of the paper is the most common language of the paragraphs.
- The paper must have at least 500 whitespace-separated words.
- The paper was published after 1969; papers published before this date are often obtained through OCR and contain unrecoverable errors.
- The paper must have at least 5 paragraphs.
  - All sections that have a average log word probability of less than `-20` are removed.
  - To calculate the average log word probability, we use word frequencies extracted from the [1T Web Ngram corpus](https://catalog.ldc.upenn.edu/LDC2006T13); specifically, we use the list available [created by Rachel Tatman](https://www.kaggle.com/datasets/rtatman/english-word-frequency). A copy is hosted [here](https://ai2-s2-research-public.s3-us-west-2.amazonaws.com/lucas/google-1T-unigram/unigram_freq.csv).
- The most frequent word in the paper consists of alpha characters only, and it appears in less than 7.5% of the document.
  - Words are obtained by splitting the text on whitespace.

The train set contains papers published before 2022-12-01;
the validation set includes documents published after 2022-12-01 and until 2023-01-03.

#### S2AG-derived documents

The S2AG corpus contains titles and abstracts of papers in Semantic Scholar.
Unfiltered, the corpus contains 91.1M papers and 15.5B whitespace-separated tokens as of 2023-01-03. To derive peS2o v1, we impose the following constraints:

- Abstract must be in English.
  - To calculate the language, we once again use pycld3
- Title must be in English, or have average unigram log probability greater than -20.
- Abstract must be in English.
- Abstract must have higher than -20 average unigram log probability.
- Abstract must have at least 50 words.
- Abstract must have no more than 1000 words.
- The most frequent word in the union of text and abstract must be a 2+ character alpha word, or it can be `a` followed by a 2+ character alpha word.
- Paper was published after 1969.

#### Statistics

| Dataset | Split   | # Documents | # Words        |
|:-------:|:-------:|:-----------:|:--------------:|
|s2orc    | train   | 8,242,162   | 36,088,195,908 |
|s2orc    | valid   | 51,323      | 255,139,074    |
|s2ag     | train   | 59,382,301  | 11,009,123,378 |
|s2ag     | valid   | 111,228     | 24,398,512     |


------

## peS2o V2


### Key Facts

- *Knowledge cutoff*: 2023-01-03
- *Number of documents*: 38.97M
- *Number of whitespace-separated tokens**: 42.01B

### Processing

peS2o V2 is largely the same as V1, but it includes additional heuristics s2ag aimed at filtering out OCR errors from abstract.

First, we check if the abstract was obtained from Semantic Scholar sources that are likely to contain OCR'ed content. For any abstract derived from those sources, we count how often the text contains subsequences matching `\b([A-Za-z]\s)([a-z]\s)*[A-Za-z]\b`, i.e. individual alpha letters separated by a space. This heuristic matches cases such as `A b stra ct` (2 matching subsequences), where the OCR parser inserted erroneous spaces.
Any abstract with more than 4 matching subsequences is removed.


#### Statistics

| Dataset | Split | # Documents | # Words        |
|:-------:|:-----:|------------:|---------------:|
| s2orc   | train |  8,242,162  | 36,088,195,908 |
| s2orc   | valid |     51,323  |    255,139,074 |
| s2ag    | train | 30,569,017  |  5,920,099,207 |
| s2ag    | valid |    109,709  |     24,029,459 |

[1]: https://aclanthology.org/2020.acl-main.447/
[2]: https://github.com/allenai/s2orc
