use super::*;
use std::fs;

#[test]
fn test_japanese_column_names_and_identifiers() {
    let rows = 100;
    let mut text = String::from("氏名,年齢,部署\n");
    for i in 0..rows {
        text.push_str(&format!("ユーザー{},{},開発部\n", i, 20 + (i % 30)));
    }
    let f = TempCsv(gendata::write_temp_bytes("japanese_cols", text.as_bytes()));
    let p = f.0.display();

    // Query using Japanese column names in project and filter
    let res = run_src(
        &format!("
            O:
              open {p}
              |? 年齢 >= 30
              |> 氏名 年齢 部署
            ;
        "),
        10,
    );

    assert!(res.errors.is_empty());
    
    // Check that we filtered correctly
    let ages = collect_i64(&res, "O", "年齢");
    for age in ages {
        assert!(age >= 30);
    }
}

#[test]
fn test_japanese_file_paths() {
    let dir = std::env::temp_dir().join("テスト_ディレクトリ");
    let _ = fs::create_dir_all(&dir);
    let file_path = dir.join("データ_入力.csv");
    let text = "id,値\n1,あいう\n2,かきく\n";
    fs::write(&file_path, text).unwrap();

    let p = file_path.to_string_lossy().replace('\\', "/");
    let res = run_src(
        &format!("
            O:
              open {p}
              |? id == 1
              |> 値
            ;
        "),
        5,
    );

    assert!(res.errors.is_empty());
    let vals = collect_strings(&res, "O", "値");
    assert_eq!(vals, vec!["あいう"]);

    let _ = fs::remove_file(&file_path).unwrap();
    let _ = fs::remove_dir(&dir).unwrap();
}

#[test]
fn test_windows_long_path_support() {
    // Construct a path that is very long (exceeds 300 characters)
    let mut dir = std::env::temp_dir();
    for i in 0..15 {
        dir = dir.join(format!("long_path_directory_segment_number_{i}"));
    }
    
    // Create the deep directory using standard fs::create_dir_all
    fs::create_dir_all(&dir).unwrap();

    let file_path = dir.join("test_file.csv");

    let text = "col_a,col_b\n100,200\n";
    fs::write(&file_path, text).unwrap();

    // Read the file using standard open which uses adjust_path internally
    let p = file_path.to_string_lossy().replace('\\', "/");
    let res = run_src(
        &format!("
            O:
              open {p}
              |> col_a col_b
            ;
        "),
        5,
    );

    assert!(res.errors.is_empty());
    let a_vals = collect_i64(&res, "O", "col_a");
    assert_eq!(a_vals, vec![100]);

    // Clean up
    let _ = fs::remove_file(&file_path).unwrap();
    // Recursively delete the nested directories
    let mut current = dir;
    while current != std::env::temp_dir() {
        let _ = fs::remove_dir(&current);
        if let Some(parent) = current.parent() {
            current = parent.to_path_buf();
        } else {
            break;
        }
    }
}
