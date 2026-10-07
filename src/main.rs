use k256::elliptic_curve::sec1::ToEncodedPoint;
use k256::elliptic_curve::PrimeField;
use k256::{ProjectivePoint, Scalar, AffinePoint};
use num_bigint::BigUint;
use rayon::prelude::*;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::sync::Mutex;

fn main() {
    // Command line arguments ကနေ Shard Index နဲ့ Total Shards ကို ဖမ်းယူရန် (GitHub Actions Matrix အတွက်)
    let args: Vec<String> = env::args().collect();
    let shard_id: u64 = args.get(1).unwrap_or(&String::from("0")).parse().unwrap_or(0);
    let total_shards: u64 = args.get(2).unwrap_or(&String::from("1")).parse().unwrap_or(1);

    let base_k_str = "696898287454081973172991196020261297061888";
    let base_bigint = base_k_str.parse::<BigUint>().expect("Invalid base_k number");

    let start_offset: u64 = 140_000_000_000; //စတင်မည့် နေရာ
    let total_max_steps: u64 = 5_000_000_000; // စုစုပေါင်း လုပ်ရမည့် steps ပမာဏ
    
    // Shard တစ်ခုချင်းစီအတွက် တာဝန်ကျမယ့် steps ပမာဏကို ခွဲဝေခြင်း
    let steps_per_shard = total_max_steps / total_shards;
    let my_start_offset = start_offset + (shard_id * steps_per_shard);
    let my_max_steps = if shard_id == total_shards - 1 {
        total_max_steps - (shard_id * steps_per_shard)
    } else {
        steps_per_shard
    };

    let chunk_size: u64 = 10_000_000;
    let table_filename = format!("trap_table8_shard_{}.bin", shard_id); // Shard အလိုက် ဖိုင်ခွဲသိမ်းမည်

    let mut bytes = [0u8; 32];
    let bigint_bytes = base_bigint.to_bytes_be();
    bytes[32 - bigint_bytes.len()..].copy_from_slice(&bigint_bytes);
    let base_scalar = Scalar::from_repr(bytes.into()).unwrap();

    let table_file = OpenOptions::new().create(true).append(true).open(&table_filename).unwrap();
    let table_file_arc = Arc::new(Mutex::new(table_file));

    println!("==================================================");
    println!("🚀 Shard {}/{} | Start Offset: {} | Steps: {}", shard_id + 1, total_shards, my_start_offset, my_max_steps);
    println!("Target File: {}", table_filename);
    println!("==================================================");

    let found = Arc::new(AtomicBool::new(false));
    let num_threads = num_cpus::get();
    let start_time = Instant::now();
    let mut current_step: u64 = 0;

    while current_step < my_max_steps && !found.load(Ordering::Relaxed) {
        let current_chunk = std::cmp::min(chunk_size, my_max_steps - current_step);
        let batch_start_offset = my_start_offset + current_step;
        let table_file_thread = Arc::clone(&table_file_arc);

        (0..num_threads).into_par_iter().for_each(|thread_id| {
            if found.load(Ordering::Relaxed) { return; }

            let thread_step_offset = thread_id as u64 * (current_chunk / num_threads as u64);
            let actual_chunk = if thread_id == num_threads - 1 {
                current_chunk - thread_step_offset
            } else {
                current_chunk / num_threads as u64
            };

            let absolute_offset = batch_start_offset + thread_step_offset;
            let offset_scalar = Scalar::from(absolute_offset);
            let current_scalar = base_scalar + offset_scalar;
            let mut current_point = ProjectivePoint::GENERATOR * current_scalar;
            let generator_point = ProjectivePoint::GENERATOR;

            for i in 0..actual_chunk {
                let affine = AffinePoint::from(current_point);
                let encoded = affine.to_encoded_point(false);
                let pub_bytes = encoded.as_bytes();

                if pub_bytes[1] == 0 && pub_bytes[2] == 0 && pub_bytes[3] == 0 {
                    let absolute_index = absolute_offset + i;
                    let mut x_suffix = [0u8; 8];
                    x_suffix.copy_from_slice(&pub_bytes[25..33]);

                    let mut record = [0u8; 16];
                    record[0..8].copy_from_slice(&x_suffix);
                    record[8..16].copy_from_slice(&absolute_index.to_le_bytes());

                    let mut file = table_file_thread.lock().unwrap();
                    file.write_all(&record).unwrap();

                    println!("🎯 Found & Saved 16-byte record at offset: {}", absolute_index);
                }
                current_point += generator_point;
            }
        });

        current_step += current_chunk;
        let elapsed = start_time.elapsed().as_secs_f64();
        println!("Progress: {} / {} steps | Speed: {:.2} keys/sec", current_step, my_max_steps, current_step as f64 / elapsed);
    }
    println!("\n✅ Shard {} finished successfully.", shard_id);
  }
