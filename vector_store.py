import time
import logging
from typing import List, Dict, Any, Optional
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA
from langchain.vectorstores.base import VectorStore
from langchain.schema import Document
from langchain_openai import AzureOpenAIEmbeddings

from qdrant_client import QdrantClient, models
from qdrant_client.models import Distance, VectorParams, PointStruct, SearchParams

from validation import TextQualityAnalyzer, FieldExtractionResult, ExtractionQuality
from config import (
    QDRANT_HOST, QDRANT_PORT, QDRANT_API_KEY, COLLECTION_NAME, QDRANT_CONFIG,
    azure_endpoint, api_key, OLLAMA_LLM_MODEL, FIELD_EXTRACTION_CONFIG
)

logger = logging.getLogger(__name__)

class EnhancedQdrantVectorStore(VectorStore):
    """Enhanced Qdrant vector store optimized for medical document accuracy."""
    
    def __init__(
        self, 
        client: QdrantClient, 
        collection_name: str, 
        embedding_function,
        config: Dict[str, Any] = None
    ):
        self.client = client
        self.collection_name = collection_name
        self.embedding_function = embedding_function
        self.config = config or QDRANT_CONFIG
        
    def add_texts(
        self, 
        texts: List[str], 
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs
    ) -> List[str]:
        """Add texts with enhanced batching and error handling."""
        if not texts:
            return []
            
        if ids is None:
            ids = [str(i) for i in range(len(texts))]
            
        if metadatas is None:
            metadatas = [{}] * len(texts)
        
        logger.info(f"Adding {len(texts)} texts to Qdrant collection {self.collection_name}")
        
        # Process in smaller batches for reliability
        batch_size = 25  # Smaller batches for better reliability
        all_points = []
        
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_metadatas = metadatas[i:i + batch_size]
            batch_ids = ids[i:i + batch_size]
            
            try:
                # Get embeddings for this batch
                embeddings = self.embedding_function.embed_documents(batch_texts)
                
                # Create points
                for text, embedding, metadata, point_id in zip(batch_texts, embeddings, batch_metadatas, batch_ids):
                    all_points.append(
                        PointStruct(
                            id=str(point_id),
                            vector=embedding,
                            payload={
                                "text": text,
                                "text_length": len(text),
                                "word_count": len(text.split()),
                                **metadata
                            }
                        )
                    )
                    
                logger.info(f"Processed embedding batch {i//batch_size + 1}/{(len(texts)-1)//batch_size + 1}")
                
            except Exception as e:
                logger.error(f"Failed to process batch {i//batch_size + 1}: {e}")
                continue
        
        # Insert points into Qdrant in batches
        insert_batch_size = 50
        inserted_count = 0
        
        for i in range(0, len(all_points), insert_batch_size):
            batch = all_points[i:i + insert_batch_size]
            
            try:
                self.client.upsert(
                    collection_name=self.collection_name,
                    points=batch,
                    wait=True  # Wait for confirmation
                )
                inserted_count += len(batch)
                logger.info(f"Inserted batch {i//insert_batch_size + 1}/{(len(all_points)-1)//insert_batch_size + 1} "
                           f"({inserted_count}/{len(all_points)} points)")
                
            except Exception as e:
                logger.error(f"Failed to insert batch {i//insert_batch_size + 1}: {e}")
                continue
        
        logger.info(f"Successfully added {inserted_count}/{len(all_points)} points to Qdrant")
        return ids[:inserted_count]

    def similarity_search(
        self, 
        query: str, 
        k: int = 4,
        filter: Optional[dict] = None,
        **kwargs
    ) -> List[Document]:
        """Enhanced similarity search with better error handling."""
        
        try:
            query_vector = self.embedding_function.embed_query(query)
            
            # Configure search parameters for maximum accuracy
            search_params = SearchParams(
                hnsw_ef=self.config.get("hnsw_ef_search", 200),  # Higher for accuracy
                exact=False,
                quantization=None  # Disabled for maximum accuracy
            )
            
            # Perform search with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    results = self.client.search(
                        collection_name=self.collection_name,
                        query_vector=query_vector,
                        limit=k,
                        search_params=search_params,
                        query_filter=filter,
                        with_payload=True,
                        with_vectors=False
                    )
                    break
                    
                except Exception as e:
                    logger.warning(f"Search attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        logger.error(f"All search attempts failed for query: {query[:100]}...")
                        return []
                    time.sleep(1)
            
            # Convert results to LangChain documents
            documents = []
            for result in results:
                doc = Document(
                    page_content=result.payload.get("text", ""),
                    metadata={
                        "score": result.score,
                        "text_length": result.payload.get("text_length", 0),
                        "word_count": result.payload.get("word_count", 0),
                        **{k: v for k, v in result.payload.items() if k not in ["text", "text_length", "word_count"]}
                    }
                )
                documents.append(doc)
            
            logger.info(f"Retrieved {len(documents)} documents for query")
            return documents
            
        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return []

    def as_retriever(self, **kwargs):
        """Return enhanced retriever interface."""
        from langchain.schema.retriever import BaseRetriever
        
        class EnhancedQdrantRetriever(BaseRetriever):
            def __init__(self, vectorstore, search_kwargs=None):
                self.vectorstore = vectorstore
                self.search_kwargs = search_kwargs or {}
            
            def _get_relevant_documents(self, query: str) -> List[Document]:
                return self.vectorstore.similarity_search(query, **self.search_kwargs)
        
        return EnhancedQdrantRetriever(self, kwargs)

    @classmethod
    def from_texts(
        cls,
        texts: List[str],
        embedding,
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        collection_name: str = COLLECTION_NAME,
        **kwargs
    ):
        """Create enhanced QdrantVectorStore with your credentials."""
        
        # Initialize Qdrant client with your credentials
        try:
            client = QdrantClient(
                url=f"https://{QDRANT_HOST}",
                port=QDRANT_PORT,
                api_key=QDRANT_API_KEY,
                https=True,
                timeout=60
            )
            
            # Test connection
            collections = client.get_collections()
            logger.info(f"Successfully connected to Qdrant at {QDRANT_HOST}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Qdrant: {e}")
            raise
        
        # Create or verify collection
        try:
            collection_info = client.get_collection(collection_name)
            logger.info(f"Using existing collection: {collection_name}")
            logger.info(f"Collection status: {collection_info.status}")
            
        except Exception:
            logger.info(f"Creating new collection: {collection_name}")
            
            # Get embedding dimension
            try:
                sample_embedding = embedding.embed_query("sample medical document for dimension testing")
                vector_size = len(sample_embedding)
                logger.info(f"Detected embedding dimension: {vector_size}")
                
            except Exception as e:
                logger.error(f"Failed to get embedding dimension: {e}")
                raise
            
            try:
                client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(
                        size=vector_size,
                        distance=Distance.COSINE
                    ),
                    hnsw_config=models.HnswConfigDiff(
                        ef_construct=QDRANT_CONFIG.get("hnsw_ef_construct", 400),
                        m=QDRANT_CONFIG.get("hnsw_m", 64)
                    ),
                    # No quantization for maximum accuracy
                    quantization_config=None
                )
                
                logger.info(f"Successfully created collection {collection_name}")
                
            except Exception as e:
                logger.error(f"Failed to create collection: {e}")
                raise
        
        # Create vector store instance
        vector_store = cls(client, collection_name, embedding, QDRANT_CONFIG)
        
        # Add texts if provided
        if texts:
            vector_store.add_texts(texts, metadatas, ids)
        
        return vector_store

def build_enhanced_vectordb_with_qdrant(all_texts: List[str], collection_name: str = COLLECTION_NAME):
    """Build enhanced vector database with medical document optimization."""
    
    if not all_texts:
        logger.warning("No texts provided for vector database")
        return None
    
    logger.info(f"Building enhanced vector database with {len(all_texts)} documents")
    start_time = time.time()
    
    # Enhanced text splitter for medical documents
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1500,  # Larger chunks for better medical context
        chunk_overlap=300,  # More overlap for medical continuity
        separators=[
            "\n\n--- Page", "\n\n", "\n", 
            ". ", "? ", "! ",  # Sentence boundaries
            "; ", ", ",        # Clause boundaries
            " ", ""            # Word and character boundaries
        ],
        length_function=len,
        is_separator_regex=False
    )
    
    all_chunks = []
    metadatas = []
    chunk_quality_scores = []
    
    # Process each document
    for doc_idx, text in enumerate(all_texts):
        if not text.strip():
            continue
            
        # Analyze document quality first
        doc_quality = TextQualityAnalyzer.analyze_comprehensive(text)
        
        # Split into chunks
        chunks = splitter.split_text(text)
        
        for chunk_idx, chunk in enumerate(chunks):
            # Analyze chunk quality
            chunk_quality = TextQualityAnalyzer.analyze_comprehensive(chunk)
            
            # Only include high-quality chunks
            if chunk_quality["score"] >= 30:  # Minimum quality threshold
                all_chunks.append(chunk)
                metadatas.append({
                    "doc_index": doc_idx,
                    "chunk_index": chunk_idx,
                    "doc_quality_score": doc_quality["score"],
                    "chunk_quality_score": chunk_quality["score"],
                    "medical_indicators": chunk_quality.get("medical_indicators", 0),
                    "word_count": chunk_quality.get("word_count", 0)
                })
                chunk_quality_scores.append(chunk_quality["score"])
    
    if not all_chunks:
        logger.error("No quality chunks found for vector database")
        return None
    
    avg_quality = sum(chunk_quality_scores) / len(chunk_quality_scores)
    logger.info(f"Created {len(all_chunks)} quality chunks (avg quality: {avg_quality:.1f})")
    
    # Initialize Azure OpenAI embeddings
    embeddings = AzureOpenAIEmbeddings(
        azure_endpoint=azure_endpoint,
        api_key=api_key,
        deployment="text-embedding-ada-002",
        api_version="2023-05-15"
    )
    
    # Create enhanced Qdrant vector store
    try:
        vectordb = EnhancedQdrantVectorStore.from_texts(
            all_chunks,
            embeddings,
            metadatas=metadatas,
            collection_name=collection_name
        )
        
        build_time = time.time() - start_time
        logger.info(f"Enhanced vector database built in {build_time:.2f} seconds")
        logger.info(f"Configuration: ef_construct={QDRANT_CONFIG['hnsw_ef_construct']}, "
                   f"m={QDRANT_CONFIG['hnsw_m']}, ef_search={QDRANT_CONFIG['hnsw_ef_search']}")
        logger.info(f"Connected to: {QDRANT_HOST}")
        
        return vectordb
        
    except Exception as e:
        logger.error(f"Failed to build vector database: {e}")
        return None

def enhanced_rag_extract_fields_v2(text: str, vectordb, doc_id: str, max_retries: int = 5) -> FieldExtractionResult:
    """Enhanced RAG extraction with comprehensive medical document understanding."""
    
    if not text.strip():
        return FieldExtractionResult(
            fields=AccuracyFocusedFieldExtractor()._get_empty_fields_structure(),
            confidence=0.0,
            method="no_text",
            validation_errors=["No text provided"],
            quality=ExtractionQuality.FAILED
        )
    
    # Analyze text quality first
    text_quality = TextQualityAnalyzer.analyze_comprehensive(text)
    logger.info(f"Text quality for {doc_id}: {text_quality['score']:.1f} ({text_quality['quality'].value})")
    
    if text_quality["score"] < 25:
        logger.warning(f"Very low text quality for {doc_id}, extraction may be unreliable")
    
    # Enhanced prompt template with medical expertise
    enhanced_prompt_template = """
You are a highly specialized medical records extraction expert with deep knowledge of healthcare documentation standards.

CRITICAL EXTRACTION REQUIREMENTS:
- Extract information ONLY if it is CLEARLY and UNAMBIGUOUSLY present in the document
- Use medical document expertise to identify relevant information patterns
- Apply healthcare industry knowledge for field interpretation
- Maintain strict accuracy standards - if uncertain, use null

FIELD SPECIFICATIONS:
- orderno: Medical order number (often alphanumeric, may include prefixes like "ORD", "NOF", etc.)
- orderdate: Date the medical order was issued (MM/DD/YYYY format)
- mrn: Medical Record Number - unique patient identifier (alphanumeric, typically 6-12 characters)
- soc: Start of Care date - when medical services begin (MM/DD/YYYY format)
- cert_period: Certification period with exact dates
  - soe: Start of Episode date (MM/DD/YYYY format)
  - eoe: End of Episode date (MM/DD/YYYY format)
- icd_codes: ICD-10 diagnosis codes (format: letter + 2 digits + optional decimal + 1-2 more digits)
- patient_name: Full legal name of the patient
- dob: Patient's date of birth (MM/DD/YYYY format)
- address: Complete patient address including street, city, state, zip
- patient_sex: Patient gender (strictly "MALE" or "FEMALE")

MEDICAL CONTEXT UNDERSTANDING:
- Episode dates typically span 60-90 days for home health services
- Start of Care often matches Start of Episode for new patients
- Order dates should precede or match care dates
- MRN formats vary by healthcare system but are always unique identifiers
- ICD-10 codes follow specific format rules and medical classification

Use the following context from similar medical documents to inform your extraction:

{context}

DOCUMENT TO PROCESS:
{question}

Return ONLY valid JSON with the exact structure specified above. No explanations, no additional text.

JSON:"""

    try:
        # Initialize Ollama with optimal settings for medical document processing
        llm = Ollama(
            model=OLLAMA_LLM_MODEL,
            temperature=0.05,  # Very low temperature for maximum consistency
            top_k=5,           # Limited vocabulary for focused responses
            top_p=0.8,         # Nucleus sampling for quality
            repeat_penalty=1.1, # Prevent repetition
            num_ctx=4096,      # Large context window for medical documents
        )
        
        prompt = PromptTemplate.from_template(enhanced_prompt_template)
        
        # Enhanced retrieval strategy
        retriever = vectordb.as_retriever(
            search_kwargs={
                "k": 8,  # Retrieve more context for better accuracy
                "filter": None  # Could add filters based on document type
            }
        )
        
        qa = RetrievalQA.from_chain_type(
            llm=llm,
            retriever=retriever,
            chain_type="stuff",
            return_source_documents=True,  # Get source documents for analysis
            chain_type_kwargs={"prompt": prompt}
        )
        
        # Create focused medical query
        medical_keywords = ["patient", "diagnosis", "medical", "order", "care", "episode", "certification"]
        text_keywords = [word for word in medical_keywords if word.lower() in text.lower()]
        
        focused_query = f"Extract medical order information from document containing: {', '.join(text_keywords)}. Document preview: {text[:800]}..."
        
        best_result = None
        best_confidence = 0
        validation_errors = []
        
        for attempt in range(max_retries):
            try:
                logger.info(f"RAG extraction attempt {attempt + 1} for {doc_id}")
                
                response = qa(focused_query)
                
                # Handle response format
                if isinstance(response, dict):
                    result_text = response.get("result", str(response))
                    source_docs = response.get("source_documents", [])
                else:
                    result_text = str(response)
                    source_docs = []
                
                logger.info(f"Retrieved {len(source_docs)} source documents for context")
                
                # Extract and validate JSON
                json_match = re.search(r'\{[\s\S]*\}', result_text)
                if json_match:
                    json_str = json_match.group()
                    try:
                        parsed_result = json.loads(json_str)
                        
                        # Validate structure
                        field_extractor = AccuracyFocusedFieldExtractor()
                        if field_extractor._validate_extraction_structure(parsed_result):
                            
                            # Comprehensive field validation
                            confidence, errors = MedicalFieldValidator.validate_fields_comprehensive(parsed_result)
                            
                            logger.info(f"Attempt {attempt + 1} confidence: {confidence:.2f}")
                            
                            # Keep best result based on confidence
                            if confidence > best_confidence:
                                best_confidence = confidence
                                best_result = parsed_result
                                validation_errors = errors
                            
                            # If we have high confidence, stop trying
                            if confidence >= FIELD_EXTRACTION_CONFIG.get("field_confidence_threshold", 0.7):
                                logger.info(f"High confidence achieved ({confidence:.2f}), stopping attempts")
                                break
                        
                        else:
                            logger.warning(f"Attempt {attempt + 1}: Invalid structure")
                    
                    except json.JSONDecodeError as e:
                        logger.warning(f"Attempt {attempt + 1}: JSON parsing failed - {e}")
                
                else:
                    logger.warning(f"Attempt {attempt + 1}: No JSON found in response")
                
                # Small delay between attempts
                if attempt < max_retries - 1:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"RAG attempt {attempt + 1} failed for {doc_id}: {e}")
                continue
        
        # Determine final result
        if best_result:
            logger.info(f"RAG extraction completed for {doc_id} with confidence {best_confidence:.2f}")
            
            # Determine quality based on confidence and validation
            if best_confidence >= 0.85:
                quality = ExtractionQuality.EXCELLENT
            elif best_confidence >= 0.7:
                quality = ExtractionQuality.GOOD
            elif best_confidence >= 0.5:
                quality = ExtractionQuality.FAIR
            elif best_confidence >= 0.3:
                quality = ExtractionQuality.POOR
            else:
                quality = ExtractionQuality.FAILED
            
            return FieldExtractionResult(
                fields=best_result,
                confidence=best_confidence,
                method="enhanced_rag",
                validation_errors=validation_errors,
                quality=quality
            )
        
        else:
            logger.error(f"All RAG extraction attempts failed for {doc_id}")
            return FieldExtractionResult(
                fields=AccuracyFocusedFieldExtractor()._get_empty_fields_structure(),
                confidence=0.0,
                method="rag_failed",
                validation_errors=["All RAG extraction attempts failed"],
                quality=ExtractionQuality.FAILED
            )
            
    except Exception as e:
        logger.error(f"Critical RAG extraction error for {doc_id}: {e}")
        return FieldExtractionResult(
            fields=AccuracyFocusedFieldExtractor()._get_empty_fields_structure(),
            confidence=0.0,
            method="rag_critical_error",
            validation_errors=[f"Critical error: {str(e)}"],
            quality=ExtractionQuality.FAILED
        ) 