from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import logging
import pandas as pd
import subprocess
from typing import List
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from jinja2 import Template

# ✅ FastAPI 앱 생성
app = FastAPI()

COST_CSV_FILE = "사전원가.csv"
PRICE_CSV_FILE = "단가누락.csv"

# ✅ CORS 설정 추가 (프론트엔드 접근 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 보안을 위해 배포 시 특정 도메인만 허용
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ 업로드 및 처리된 파일 저장 폴더 설정
UPLOAD_DIR = "uploads"
RESULT_DIR = "results"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

# ✅ 로그 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@app.post("/upload/")
async def upload_files(files: List[UploadFile] = File(...)):
    """
    여러 개의 파일 업로드 및 XLSX → CSV 변환 API
    """
    uploaded_files = []

    for file in files:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        logger.info(f"파일 업로드 시작: {file.filename}")

        # 파일 저장
        with open(file_path, "wb") as f:
            f.write(await file.read())

        # ✅ XLSX → CSV 변환 (업로드 시 변환 수행)
        if file.filename.endswith(".xlsx"):
            try:
                df = pd.read_excel(file_path, engine="openpyxl")
                csv_file_path = file_path.replace(".xlsx", ".csv")
                df.to_csv(csv_file_path, index=False, encoding="utf-8-sig")
                logger.info(f"XLSX → CSV 변환 완료: {csv_file_path}")

                # 원본 XLSX 삭제 (선택 사항)
                os.remove(file_path)
                uploaded_files.append(csv_file_path.split("/")[-1])  # 변환된 CSV 파일명 저장
            except Exception as e:
                logger.error(f"XLSX 변환 실패: {str(e)}")
                raise HTTPException(status_code=500, detail=f"XLSX 변환 실패: {str(e)}")
        else:
            uploaded_files.append(file.filename)

    return {"message": "파일 업로드 및 변환 완료", "uploaded_files": uploaded_files}


# ✅ 단계별 실행 함수
def run_step(step_number: int):
    """
    특정 단계의 Python 스크립트를 실행하는 함수
    """
    script_path = f"사전원가_{step_number}단계.py"
    
    if not os.path.exists(script_path):
        logger.error(f"파일이 존재하지 않음: {script_path}")
        raise HTTPException(status_code=404, detail=f"해당 단계의 파일이 존재하지 않습니다: {script_path}")

    try:
        logger.info(f"🚀 {step_number}단계 실행 중...")
        subprocess.run(["python", script_path], check=True)  # ✅ Python 실행
        logger.info(f"✅ {step_number}단계 실행 완료")
        return {"message": f"Step {step_number} executed successfully"}
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {step_number}단계 실행 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Step {step_number} 실행 오류: {str(e)}")


@app.post("/run-step/{step_number}")
def run_single_step(step_number: int):
    """
    특정 단계 실행 API
    """
    return run_step(step_number)


@app.post("/run-processing/")
def run_all_steps():
    """
    0단계부터 59단계까지 순차적으로 실행하는 API
    """
    try:
        logger.info("🚀 전체 단계 실행 시작")

        for step in range(60):  # ✅ 0~59단계 실행
            run_step(step)

        logger.info("✅ 모든 단계 실행 완료")
        return {"message": "모든 단계 실행 완료"}
    
    except Exception as e:
        logger.error(f"❌ 전체 실행 중 오류 발생: {str(e)}")
        raise HTTPException(status_code=500, detail=f"전체 실행 오류: {str(e)}")

@app.get("/list-results/")
async def list_results():
    """
    처리된 파일 목록 반환 API
    """
    files = os.listdir(RESULT_DIR)
    logger.info(f"📂 현재 저장된 파일 목록: {files}")  # ✅ 디버깅용 로그 추가
    return {"processed_files": files}

@app.get("/download/{file_name}")
async def download_file(file_name: str):
    """
    처리된 파일 다운로드 API (XLSX 파일 자동 매핑)
    """
    original_file_name = file_name  # 원래 요청된 파일명 유지
    
    # ✅ CSV 요청이 들어오면 자동으로 XLSX 파일을 찾아 반환
    if file_name in ["사전원가.csv", "단가누락.csv"]:
        file_name_xlsx = file_name.replace(".csv", ".xlsx")
        file_path_xlsx = os.path.join(RESULT_DIR, file_name_xlsx)
        
        if os.path.exists(file_path_xlsx):
            logger.info(f"📥 XLSX 파일 다운로드 요청: {file_name_xlsx}")
            return FileResponse(
                file_path_xlsx,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=file_name_xlsx
            )
    
    # 기본 CSV 다운로드 처리
    file_path_csv = os.path.join(RESULT_DIR, file_name)
    if not os.path.exists(file_path_csv):
        logger.error(f"❌ CSV 파일 다운로드 실패: {original_file_name} (파일이 존재하지 않음)")
        raise HTTPException(status_code=404, detail="파일이 존재하지 않습니다.")

    logger.info(f"📥 CSV 파일 다운로드 요청: {file_name}")
    return FileResponse(file_path_csv, media_type="text/csv", filename=file_name)

# ✅ 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>엑셀 데이터 조회</title>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css">
    <script>
        $(document).ready(function() {
            $('.dataframe').DataTable({
                "paging": true,
                "searching": true,
                "ordering": true,
                "info": true,
                "lengthMenu": [10, 25, 50, 100],
                "language": {
                    "lengthMenu": "페이지당 _MENU_ 개씩 보기",
                    "zeroRecords": "🔍 검색 결과가 없습니다.",
                    "info": "총 _TOTAL_ 개 중 _START_ - _END_",
                    "infoEmpty": "데이터가 없습니다.",
                    "infoFiltered": "(총 _MAX_ 개 중 검색)",
                    "search": "🔍 검색: ",
                    "paginate": {
                        "first": "처음",
                        "last": "마지막",
                        "next": "다음",
                        "previous": "이전"
                    }
                }
            });
        });
    </script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: center; }
        th { background-color: #f4f4f4; }
        .empty-message { text-align: center; font-weight: bold; color: #888; margin-top: 10px; }
    </style>
</head>
<body>
    <h2>단가누락 데이터</h2>
    {{ table1 | safe }}
    {{ empty_message1 | safe }}

    <h2>사전원가 데이터</h2>
    {{ table2 | safe }}
    {{ empty_message2 | safe }}
</body>
</html>
"""

@app.get("/view-data", response_class=HTMLResponse)
def view_data():
    try:
        file1 = os.path.join(RESULT_DIR, "단가누락.xlsx")
        file2 = os.path.join(RESULT_DIR, "사전원가.xlsx")

        # ✅ 파일이 없을 경우 빈 데이터프레임 생성
        if not os.path.exists(file1):
            df1 = pd.DataFrame(columns=["품목대분류", "품번", "품명", "자재번호", "자재명", "단가"])
            empty_message1 = "<p class='empty-message'>📂 아직 데이터가 없습니다. 파일을 업로드하세요.</p>"
        else:
            df1 = pd.read_excel(file1)
            empty_message1 = ""

        if not os.path.exists(file2):
            df2 = pd.DataFrame(columns=["품목대분류", "품번", "품명", "자재번호", "자재명", "단가"])
            empty_message2 = "<p class='empty-message'>📂 아직 데이터가 없습니다. 파일을 업로드하세요.</p>"
        else:
            df2 = pd.read_excel(file2)
            empty_message2 = ""

        # ✅ HTML 테이블 변환 (검색 및 정렬 가능)
        table1 = df1.to_html(index=False, classes="dataframe")
        table2 = df2.to_html(index=False, classes="dataframe")

        # ✅ HTML 렌더링
        template = Template(html_template)
        return template.render(table1=table1, table2=table2, empty_message1=empty_message1, empty_message2=empty_message2)

    except Exception as e:
        return f"<h3>❌ 오류 발생: {str(e)}</h3>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
