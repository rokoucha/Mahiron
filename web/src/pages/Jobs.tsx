import { useEffect } from "react";
import { api } from "../api";
import type { DashboardState } from "../dashboard";
import { useAsync } from "../hooks";
import { ErrorList, formatDate, jobStatusLabel, PageFrame } from "../shared";

export default function Jobs({ dashboard }: { dashboard: DashboardState }) {
  const { jobs } = dashboard;
  const schedules = useAsync(api.schedules);

  useEffect(() => {
    if (dashboard.lastEvent?.resource !== "job_schedule") return;
    api.schedules().then(schedules.setData).catch(() => undefined);
  }, [dashboard.lastEvent?.resource, dashboard.lastEvent?.time, schedules.setData]);

  async function runAction(label: string, action: () => Promise<void>) {
    if (!window.confirm(`${label}?`)) return;
    await action();
    await jobs.reload();
    schedules.setData(await api.schedules());
  }

  return (
    <PageFrame title="ジョブ" subtitle="現在のジョブ、スケジュール、手動実行を確認できます。">
      <ErrorList errors={[jobs.error, schedules.error]} />
      <section className="table-section">
        <h2>ジョブ</h2>
        <table>
          <thead>
            <tr>
              <th>名前</th>
              <th>状態</th>
              <th>更新日時</th>
              <th>時間</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(jobs.data ?? []).map((job) => (
              <tr key={job.id}>
                <td>
                  <strong>{job.name}</strong>
                  <span>{job.key}</span>
                  {job.error && <em>{job.error}</em>}
                </td>
                <td><span className={`badge ${job.hasFailed ? "bad" : job.status}`}>{job.hasFailed ? "失敗" : jobStatusLabel(job.status)}</span></td>
                <td>{formatDate(job.updatedAt)}</td>
                <td>{job.duration ? `${Math.round(job.duration / 1000)}秒` : "-"}</td>
                <td className="actions">
                  <button onClick={() => runAction(`${job.name} を再実行`, () => api.rerunJob(job.id))} type="button">再実行</button>
                  {job.status !== "finished" && <button onClick={() => runAction(`${job.name} を中止`, () => api.abortJob(job.id))} type="button">中止</button>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="table-section">
        <h2>スケジュール</h2>
        <table>
          <thead>
            <tr>
              <th>キー</th>
              <th>スケジュール</th>
              <th>ジョブ</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            {(schedules.data ?? []).map((schedule) => (
              <tr key={schedule.key}>
                <td>{schedule.key}</td>
                <td>{schedule.schedule}</td>
                <td>{schedule.job.name}</td>
                <td className="actions">
                  <button onClick={() => runAction(`${schedule.key} を実行`, () => api.runSchedule(schedule.key))} type="button">実行</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </PageFrame>
  );
}
