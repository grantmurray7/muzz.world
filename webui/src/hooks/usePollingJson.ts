import { useEffect, useState } from "react";

interface PollingState<T> {
  data: T;
  loading: boolean;
  error: string;
}

export function usePollingJson<T>(url: string, initialData: T, intervalMs: number): PollingState<T> {
  const [data, setData] = useState<T>(initialData);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let active = true;

    const load = async () => {
      try {
        const response = await fetch(url, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`${response.status} ${response.statusText}`);
        }
        const payload = (await response.json()) as T;
        if (!active) {
          return;
        }
        setData(payload);
        setError("");
      } catch (err) {
        if (!active) {
          return;
        }
        setError(err instanceof Error ? err.message : "Unknown error");
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    void load();
    const timer = window.setInterval(() => {
      void load();
    }, intervalMs);

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [intervalMs, url]);

  return { data, loading, error };
}
