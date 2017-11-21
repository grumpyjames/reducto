package net.digihippo.timecache;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class InstallationProgress
{
    private final Set<String> remainingAgents;
    private final InstallationListener listener;
    private final Map<String, String> errors = new HashMap<>();

    InstallationProgress(Set<String> remainingAgents, InstallationListener listener)
    {
        this.remainingAgents = remainingAgents;
        this.listener = listener;
    }

    void complete(String agentName)
    {
        remainingAgents.remove(agentName);
        tryCompletion();
    }

    void error(String agentName, String errorMessage)
    {
        remainingAgents.remove(agentName);
        errors.put(agentName, errorMessage);
        tryCompletion();
    }

    private void tryCompletion()
    {
        if (remainingAgents.isEmpty())
        {
            if (errors.isEmpty())
            {
                listener.onComplete.run();
            }
            else
            {
                listener.onError.accept(errorMessage(errors));
            }
        }
    }

    private String errorMessage(Map<String, String> errors)
    {
        final StringBuilder result = new StringBuilder("Installation failed due to: ");
        for (Map.Entry<String, String> errorByAgent : errors.entrySet())
        {
            result.append(errorByAgent.getKey()).append(": ").append(errorByAgent.getValue()).append("\n");
        }
        return result.toString();
    }
}
