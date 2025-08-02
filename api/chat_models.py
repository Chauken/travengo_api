from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class MessageData(BaseModel):
    content: Optional[str] = None
    additional_kwargs: Optional[Dict[str, Any]] = None
    response_metadata: Optional[Dict[str, Any]] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    invalid_tool_calls: Optional[List[Dict[str, Any]]] = None


class ChatMessage(BaseModel):
    type: Optional[str] = None  # 'human' or 'ai'
    data: Optional[MessageData] = None


class ChatHistory(BaseModel):
    sessionId: Optional[str] = None
    messages: Optional[List[ChatMessage]] = None


class ChatHistoryResponse(BaseModel):
    chat_history: Optional[ChatHistory] = None
