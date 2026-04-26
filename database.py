from motor.motor_asyncio import AsyncIOMotorClient

class Database:
    def __init__(self, uri, db_name="AnimeBot"):
        self._client = AsyncIOMotorClient(uri)
        self._db = self._client[db_name]
        self._users = self._db.users

    async def get_user_data(self, user_id):
        user = await self._users.find_one({"user_id": user_id})
        if not user:
            return {}
        return user

    async def set_caption_movie(self, user_id, template):
        await self._users.update_one(
            {"user_id": user_id},
            {"$set": {"movie_caption": template}},
            upsert=True
        )

    async def set_caption_series(self, user_id, template):
        await self._users.update_one(
            {"user_id": user_id},
            {"$set": {"series_caption": template}},
            upsert=True
        )

    async def set_thumbnail(self, user_id, file_id):
        await self._users.update_one(
            {"user_id": user_id},
            {"$set": {"thumb_id": file_id}},
            upsert=True
        )

    async def clear_caption_movie(self, user_id):
        await self._users.update_one(
            {"user_id": user_id},
            {"$unset": {"movie_caption": ""}}
        )

    async def clear_caption_series(self, user_id):
        await self._users.update_one(
            {"user_id": user_id},
            {"$unset": {"series_caption": ""}}
        )

    async def clear_thumbnail(self, user_id):
        await self._users.update_one(
            {"user_id": user_id},
            {"$unset": {"thumb_id": ""}}
        )
