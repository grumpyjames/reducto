package net.digihippo.timecache;

public interface TimeCacheEvents
{
    void onAgentConnected();

    void loadComplete(String agentId, String cacheName, long bucketStart, long bucketEnd);

    void definitionsInstalled(String name);

    void definitionsInstalled(String agentName, String installationKlass);

    void iterationBucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey);

    TimeCacheEvents NO_OP = new TimeCacheEvents()
    {
        @Override
        public void onAgentConnected()
        {

        }

        @Override
        public void loadComplete(String agentId, String cacheName, long bucketStart, long bucketEnd)
        {

        }

        @Override
        public void definitionsInstalled(String name)
        {

        }

        @Override
        public void definitionsInstalled(String agentName, String installationKlass)
        {

        }

        @Override
        public void iterationBucketComplete(String agentId, String cacheName, long iterationKey, long currentBucketKey)
        {

        }
    };
}
