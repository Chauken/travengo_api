import json
import os
import sys
from typing import List, Dict, Any, Optional
from bson import ObjectId
from pymongo import MongoClient

# Add the project root to the Python path to fix import issues
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 

from api.models import TravelPackage, TravelPackageHistory

# Custom JSON encoder to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MongoJSONEncoder, self).default(obj)


class TravelPackageRepository:
    """Repository for managing travel package history"""
    
    def __init__(self, mongo_uri: str = "mongodb://192.168.1.57:27017/", db_name: str = "travengo", collection_name: str = "travel_packages_history"):
        self.mongo_uri = mongo_uri
        self.database_name = db_name
        self.collection_name = collection_name
        self.chat_database_name = "n8n"
        self.chat_collection_name = "n8n_chat_histories"
        self.backup_path = "models/travel_package_history.json"
        self._ensure_collection_exists()
    
    def _ensure_collection_exists(self) -> None:
        """Ensure the MongoDB collection exists and has proper indexes"""
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database_name]
            collection = db[self.collection_name]
            
            # Create indexes for faster queries
            collection.create_index("history_id", unique=True)
            collection.create_index("timestamp")
            collection.create_index("user_id")
            collection.create_index("session_id")
            
            # Also maintain a backup in JSON file
            self.backup_path = "models/travel_package_history.json"
            directory = os.path.dirname(self.backup_path)
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            # Fall back to JSON file storage
            self.mongo_uri = None
    
    def _get_collection(self):
        """Get MongoDB collection or None if not available"""
        if not self.mongo_uri:
            return None
        
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.database_name]
            return db[self.collection_name]
        except Exception as e:
            print(f"MongoDB connection error: {e}")
            return None
            
    def _get_chat_collection(self):
        """Get MongoDB chat history collection or None if not available"""
        if not self.mongo_uri:
            return None
        
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.chat_database_name]
            return db[self.chat_collection_name]
        except Exception as e:
            print(f"MongoDB connection error for chat history: {e}")
            return None
    
    def _backup_to_json(self, history: List[Dict[str, Any]]) -> None:
        """Backup history to JSON file"""
        try:
            with open(self.backup_path, 'w') as f:
                json.dump({"history": history}, f, indent=2, cls=MongoJSONEncoder)
        except Exception as e:
            print(f"Error backing up to JSON: {e}")
    
    def save_travel_packages(self, travel_packages: List[TravelPackage], 
                            user_id: Optional[str] = None,
                            session_id: Optional[str] = None) -> str:
        """
        Save travel packages to history
        
        Args:
            travel_packages: List of travel packages to save
            user_id: Optional user ID
            session_id: Optional session ID
            
        Returns:
            history_id: ID of the saved history entry
        """
        # Debug print to track session ID
        print(f"Processing request with session_id: {session_id}")
        
        collection = self._get_collection()
        history_id = None
        
        # Check if we already have an entry with this session ID
        existing_entry = None
        if session_id:
            # Debug print before searching for existing entry
            print(f"Searching for existing entry with session_id: {session_id}")
            
            # Direct MongoDB query to avoid any potential issues with the method
            if collection is not None:
                try:
                    existing_entry = collection.find_one({"session_id": session_id})
                    if existing_entry:
                        print(f"Found existing entry with history_id: {existing_entry.get('history_id')}")
                    else:
                        print(f"No existing entry found for session_id: {session_id}")
                except Exception as e:
                    print(f"Error searching for existing entry: {e}")
            
        if existing_entry:
            # Use existing history ID
            history_id = existing_entry.get("history_id")
            print(f"Using existing history_id: {history_id}")
            
            if collection is not None:
                try:
                    # Get existing travel packages
                    existing_packages = existing_entry.get("travel_packages", [])
                    
                    # Convert new packages to dict
                    new_packages = [tp.dict() for tp in travel_packages]
                    
                    # Check for duplicates and append only new packages
                    existing_package_ids = [pkg.get("package_id") for pkg in existing_packages]
                    
                    # Filter out packages with IDs that already exist
                    packages_to_append = []
                    for pkg in new_packages:
                        if pkg.get("package_id") not in existing_package_ids:
                            packages_to_append.append(pkg)
                        else:
                            print(f"Package ID {pkg.get('package_id')} already exists, skipping")
                    
                    # Combine existing and new packages
                    updated_packages = existing_packages + packages_to_append
                    
                    # Update the travel packages in MongoDB
                    result = collection.update_one(
                        {"session_id": session_id},
                        {"$set": {"travel_packages": updated_packages}}
                    )
                    print(f"Update result: matched={result.matched_count}, modified={result.modified_count}")
                    
                    # Get all history for backup
                    all_history = list(collection.find({}, {'_id': 0}))
                    self._backup_to_json(all_history)
                except Exception as e:
                    print(f"MongoDB error updating existing entry: {e}")
                    # Fall back to JSON storage for update
                    self._fallback_update_json(history_id, travel_packages)
            else:
                # Fall back to JSON storage for update
                self._fallback_update_json(history_id, travel_packages)
        else:
            # Create a new history entry
            print(f"Creating new history entry for session_id: {session_id}")
            history_entry = TravelPackageHistory(
                travel_packages=travel_packages,
                user_id=user_id,
                session_id=session_id
            )
            
            # Convert to dict for storage
            entry_dict = history_entry.dict()
            history_id = history_entry.history_id
            print(f"New history_id created: {history_id}")
            
            if collection is not None:
                # Save to MongoDB
                try:
                    collection.insert_one(entry_dict)
                    print("Inserted new document into MongoDB")
                    
                    # Get all history for backup
                    all_history = list(collection.find({}, {'_id': 0}))
                    self._backup_to_json(all_history)
                except Exception as e:
                    print(f"MongoDB error: {e}")
                    # Fall back to JSON storage
                    self._fallback_save_to_json(entry_dict)
            else:
                # Fall back to JSON storage
                self._fallback_save_to_json(entry_dict)
        
        return history_id
        
    def _fallback_update_json(self, history_id: str, travel_packages: List[TravelPackage]) -> None:
        """Fallback method to update travel packages in JSON file if MongoDB is not available"""
        try:
            # Read from JSON
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
            
            # Find entry to update
            for entry in history:
                if entry.get("history_id") == history_id:
                    # Get existing travel packages
                    existing_packages = entry.get("travel_packages", [])
                    
                    # Convert new packages to dict
                    new_packages = [tp.dict() for tp in travel_packages]
                    
                    # Check for duplicates and append only new packages
                    existing_package_ids = [pkg.get("package_id") for pkg in existing_packages]
                    
                    # Filter out packages with IDs that already exist
                    packages_to_append = []
                    for pkg in new_packages:
                        if pkg.get("package_id") not in existing_package_ids:
                            packages_to_append.append(pkg)
                    
                    # Combine existing and new packages
                    entry["travel_packages"] = existing_packages + packages_to_append
                    break
            
            # Write back to JSON
            with open(self.backup_path, 'w') as f:
                json.dump({"history": history}, f, indent=2)
                
            print(f"Updated history entry {history_id} in JSON backup")
        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            print(f"Error updating JSON backup: {e}")
        
    def _fallback_save_to_json(self, entry_dict: Dict[str, Any]) -> None:
        """Fallback method to save to JSON file if MongoDB is not available"""
        # Read existing history from JSON
        try:
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
        except (json.JSONDecodeError, FileNotFoundError):
            history = []
                
        # Add new entry
        history.append(entry_dict)
            
        # Write back to JSON
        try:
            with open(self.backup_path, 'w') as f:
                json.dump({"history": history}, f, indent=2)
        except Exception as e:
            print(f"Error writing to JSON file: {e}")
    
    def get_history(self, limit: int = 10, user_id: Optional[str] = None, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get travel package history
        
        Args:
            limit: Maximum number of history entries to return
            user_id: Optional user ID to filter by
            session_id: Optional session ID to filter by
            
        Returns:
            List of history entries
        """
        collection = self._get_collection()
        if collection is not None:
            # Query MongoDB
            try:
                query = {}
                if user_id:
                    query["user_id"] = user_id
                if session_id:
                    query["session_id"] = session_id
                    
                # Sort by timestamp (newest first)
                cursor = collection.find(query, {'_id': 0}).sort("timestamp", -1).limit(limit)
                return list(cursor)
            except Exception as e:
                print(f"MongoDB error: {e}")
                # Fall back to JSON
                return self._fallback_get_history_from_json(limit, user_id, session_id)
        else:
            # Fall back to JSON
            return self._fallback_get_history_from_json(limit, user_id, session_id)
    
    def _fallback_get_history_from_json(self, limit: int = 10, user_id: Optional[str] = None, session_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fallback method to get history from JSON file if MongoDB is not available"""
        try:
            # Read from JSON
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
                
            # Filter by user_id if provided
            if user_id:
                history = [entry for entry in history if entry.get("user_id") == user_id]
            
            # Filter by session_id if provided
            if session_id:
                history = [entry for entry in history if entry.get("session_id") == session_id]
            
            # Sort by timestamp (newest first)
            history.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            
            # Limit number of entries
            return history[:limit]
        except (json.JSONDecodeError, FileNotFoundError):
            return []
    
    def get_history_by_id(self, history_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific history entry by ID
        
        Args:
            history_id: ID of the history entry to retrieve
            
        Returns:
            History entry if found, None otherwise
        """
        collection = self._get_collection()
        if collection is not None:
            # Query MongoDB
            try:
                entry = collection.find_one({"history_id": history_id}, {'_id': 0})
                return entry
            except Exception as e:
                print(f"MongoDB error: {e}")
                # Fall back to JSON
                return self._fallback_get_history_by_id_from_json(history_id)
        else:
            # Fall back to JSON
            return self._fallback_get_history_by_id_from_json(history_id)
            
    def get_history_by_session_id(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific history entry by session ID
        
        Args:
            session_id: Session ID to retrieve history for
            
        Returns:
            History entry if found, None otherwise
        """
        collection = self._get_collection()
        if collection is not None:
            # Query MongoDB
            try:
                entry = collection.find_one({"session_id": session_id}, {'_id': 0})
                return entry
            except Exception as e:
                print(f"MongoDB error: {e}")
                # Fall back to JSON
                return self._fallback_get_history_by_session_id_from_json(session_id)
        else:
            # Fall back to JSON
            return self._fallback_get_history_by_session_id_from_json(session_id)
            
    def _fallback_get_history_by_session_id_from_json(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Fallback method to get history by session ID from JSON file if MongoDB is not available
        """
        try:
            # Read from JSON
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
                
            for entry in history:
                if entry.get("session_id") == session_id:
                    return entry
                    
            return None
        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            print(f"Error in JSON fallback: {e}")
            return None
    
    def _fallback_get_history_by_id_from_json(self, history_id: str) -> Optional[Dict[str, Any]]:
        """Fallback method to get history by ID from JSON file if MongoDB is not available"""
        try:
            # Read from JSON
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
                
            for entry in history:
                if entry.get("history_id") == history_id:
                    return entry
                    
            return None
        except (json.JSONDecodeError, FileNotFoundError):
            return None
            
    def get_chat_history_by_session_id(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get chat history by session ID from n8n database"""
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.chat_database_name]
            collection = db[self.chat_collection_name]
            
            # Find chat history by session ID
            chat_history = collection.find_one({"sessionId": session_id})
            
            # Convert MongoDB ObjectId to string
            if chat_history and "_id" in chat_history:
                chat_history["_id"] = str(chat_history["_id"])
                
            return chat_history
        except Exception as e:
            print(f"Error retrieving chat history: {e}")
            return None
    
    def delete_history(self, history_id: str) -> bool:
        """
        Delete a history entry by ID
        
        Args:
            history_id: ID of the history entry to delete
            
        Returns:
            True if deleted, False if not found
        """
        collection = self._get_collection()
        if collection is not None:
            # Delete from MongoDB
            try:
                result = collection.delete_one({"history_id": history_id})
                deleted = result.deleted_count > 0
                
                if deleted:
                    # Update backup
                    all_history = list(collection.find({}, {'_id': 0}))
                    self._backup_to_json(all_history)
                    
                return deleted
            except Exception as e:
                print(f"MongoDB error: {e}")
                # Fall back to JSON
                return self._fallback_delete_history_from_json(history_id)
        else:
            # Fall back to JSON
            return self._fallback_delete_history_from_json(history_id)
    
    def _fallback_delete_history_from_json(self, history_id: str) -> bool:
        """Fallback method to delete history from JSON file if MongoDB is not available"""
        try:
            # Read from JSON
            with open(self.backup_path, 'r') as f:
                data = json.load(f)
                history = data.get("history", [])
            
            # Find entry index
            index_to_delete = None
            for i, entry in enumerate(history):
                if entry.get("history_id") == history_id:
                    index_to_delete = i
                    break
            
            # Delete if found
            if index_to_delete is not None:
                del history[index_to_delete]
                with open(self.backup_path, 'w') as f:
                    json.dump({"history": history}, f, indent=2)
                return True
                
            return False
        except (json.JSONDecodeError, FileNotFoundError, Exception) as e:
            print(f"Error in JSON fallback: {e}")
            return False
